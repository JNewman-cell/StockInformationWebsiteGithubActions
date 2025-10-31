"""
Stock table synchronization script.

This script synchronizes stock data from ticker files with the database using a deterministic approach.
It compares the current database state with the source ticker files and performs necessary operations:
- Adds stocks from sources that are not in the database (after validation)
- Removes stocks from database that are no longer in sources
- Updates stocks that exist in both but have different data
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple, Optional

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_layer import (
    DatabaseConnectionManager,
    StocksRepository,
    Stock,
    ValidationError,
)
from utils.utils import (
    fetch_ticker_data_from_github_repo,
    validate_stock_symbols_market_cap_via_yahoo_finance_api,
)
from entities.synchronization_result import SynchronizationResult
from transformer.transformer import analyze_database_vs_source_symbols_for_synchronization_operations

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress psycopg connection pool verbose logging that exposes connection strings
logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
logging.getLogger('psycopg').setLevel(logging.ERROR)


def perform_synchronization_operations(stock_repo, sync_result, database_stocks) -> Dict[str, int]:
    """
    Execute the synchronization operations determined by the analysis.
    
    Args:
        stock_repo: Stock repository instance
        sync_result: SynchronizationResult containing operations to perform
        database_stocks: Dictionary of existing database stocks (to avoid additional queries)
        
    Returns:
        Dictionary with operation counts and results
    """
    results = {
        'added': 0,
        'deleted': 0,
        'updated': 0
    }
    
    # 1. Delete stocks that are no longer in the source (bulk operation)
    if sync_result.to_delete:
        logger.info(f"Bulk deleting {len(sync_result.to_delete)} stocks no longer in source data")
        deleted_count, failed_count = stock_repo.bulk_delete_by_symbols(list(sync_result.to_delete))
        results['deleted'] += deleted_count
        
        if deleted_count > 0:
            logger.info(f"Successfully deleted {deleted_count} stocks no longer in source")
        if failed_count > 0:
            logger.warning(f"Failed to delete {failed_count} stocks (not found in database)")
    
    # 1.1. Remove stocks that have persistent API errors (bulk operation)
    if sync_result.to_remove_due_to_errors:
        logger.info(f"Bulk removing {len(sync_result.to_remove_due_to_errors)} stocks due to persistent API errors")
        deleted_count, failed_count = stock_repo.bulk_delete_by_symbols(list(sync_result.to_remove_due_to_errors))
        results['deleted'] += deleted_count
        
        if deleted_count > 0:
            logger.info(f"Successfully removed {deleted_count} stocks due to API errors")
        if failed_count > 0:
            logger.warning(f"Failed to remove {failed_count} stocks with API errors (not found in database)")
    
    # 2. Add new stocks (with validation)
    if sync_result.to_add:
        logger.info(f"Adding {len(sync_result.to_add)} new stocks after validation")
        valid_stocks, validation_failures = validate_stock_symbols_market_cap_via_yahoo_finance_api(sync_result.to_add)
        sync_result.validation_failures.extend(validation_failures)
        
        if valid_stocks:
            # Convert validated data to Stock objects
            stocks_to_add = []
            for stock_data in valid_stocks:
                stock = Stock(
                    symbol=stock_data['symbol'],
                    company=stock_data.get('company')
                )
                stocks_to_add.append(stock)
            
            # Use bulk insert - no fallback needed with optimized bulk operations
            created_stocks = stock_repo.bulk_insert(stocks_to_add)
            results['added'] = len(created_stocks)
            logger.info(f"Successfully added {results['added']} new stocks using bulk insert")
    
    # 3. Update existing stocks (processed immediately during analysis phase)
    if sync_result.to_update:
        # Note: These stocks were not processed immediately during analysis (fallback case)
        logger.info(f"Processing remaining {len(sync_result.to_update)} stocks that need updates")
        
        # Ensure all stocks have proper timestamps
        current_time = datetime.now(timezone.utc).replace(tzinfo=None)
        for stock in sync_result.to_update:
            if stock.last_updated_at is None or stock.last_updated_at == stock.created_at:
                stock.last_updated_at = current_time
        
        # Process remaining updates using optimized bulk operation
        updated_count = stock_repo.bulk_update_stocks(sync_result.to_update)
        results['updated'] += updated_count
        logger.info(f"Successfully updated remaining {updated_count} stocks using bulk update")
    else:
        logger.info("All stock updates were processed immediately during analysis phase")
    
    # 4. Update timestamps for truly unchanged stocks (already verified against Yahoo Finance)
    if sync_result.unchanged:
        logger.info(f"Updating timestamps for {len(sync_result.unchanged)} verified unchanged stocks")
        updated_count = stock_repo.bulk_update_timestamps(sync_result.unchanged)
        logger.info(f"Successfully updated timestamps for {updated_count} unchanged stocks using bulk operation")
    
    logger.info(f"Synchronization operations complete: {results}")
    return results

def check_database_connectivity(db_manager, stock_repo):
    """Check database connectivity and table structure using data layer.
    
    Args:
        db_manager: Database connection manager
        stock_repo: Stock repository instance
        
    Returns:
        bool: True if database is accessible, False otherwise
    """
    try:
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("✓ Database connection successful")
        
        # Test repository functionality by getting count
        # This will validate that the STOCKS table exists and has the correct structure
        try:
            count = stock_repo.count()
            logger.info(f"✓ STOCKS table accessible with {count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ STOCKS table validation failed: {e}")
            logger.error("Please ensure the STOCKS table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False

def print_final_synchronization_statistics(stock_repo, operation_results, sync_result):
    """Print final synchronization statistics.
    
    Args:
        stock_repo: Stock repository instance
        operation_results: Dictionary with operation counts
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
Bulk Operation Results:
  - Added: {operation_results['added']} stocks (via bulk insert)
  - Deleted: {operation_results['deleted']} stocks (via bulk delete)
  - Updated: {operation_results['updated']} stocks (via bulk update)
  - Validation failures: {len(sync_result.validation_failures)}
    """)
    
    if sync_result.validation_failures:
        logger.warning(f"Failed validation for symbols: {', '.join(sync_result.validation_failures[:10])}"
                      f"{'...' if len(sync_result.validation_failures) > 10 else ''}")
    
    if sync_result.to_remove_due_to_errors:
        logger.info(f"Removed stocks due to persistent API errors: {', '.join(sync_result.to_remove_due_to_errors[:10])}"
                   f"{'...' if len(sync_result.to_remove_due_to_errors) > 10 else ''}")
    
    # Print success summary
    total_operations = (operation_results['added'] + operation_results['deleted'] + 
                       operation_results['updated'])
    print(f"\nSynchronization completed successfully using optimized bulk operations!")
    print(f"Total operations performed: {total_operations}")
    print(f"  - {operation_results['added']} stocks added (bulk insert)")
    print(f"  - {operation_results['deleted']} stocks deleted (bulk delete)") 
    print(f"  - {operation_results['updated']} stocks updated (bulk update)")
    
    # Get final database statistics
    try:
        final_count = stock_repo.count()
        logger.info(f"Final database state: {final_count} stocks")
        print(f"\nFinal database state: {final_count} total stocks")
    except Exception as e:
        logger.warning(f"Could not retrieve final statistics: {e}")


def main():
    """Main function for deterministic stock synchronization."""
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        stock_repo = StocksRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, stock_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        logger.info("=== Starting Deterministic Stock Synchronization ===")
        
        # 1. Fetch ticker data directly from GitHub repository (data source - our source of truth)
        logger.info("Fetching ticker data from GitHub repository...")
        source_symbols = fetch_ticker_data_from_github_repo()
        source_symbols = set(source_symbols)  # Convert to set for efficient operations
        
        logger.info(f"Loaded {len(source_symbols)} unique symbols from GitHub repository")
        
        # 2. Get current database state
        logger.info("Retrieving current database state...")
        database_stocks = stock_repo.get_all_symbols()
        
        logger.info(f"Found {len(database_stocks)} stocks currently in database")
        
        # 3. Compare and determine synchronization operations
        logger.info("Analyzing differences between sources and database...")
        
        # Create batch update function for immediate processing
        def batch_update_stocks(stocks_batch):
            return stock_repo.bulk_update_stocks(stocks_batch)
        
        sync_result = analyze_database_vs_source_symbols_for_synchronization_operations(database_stocks, source_symbols, batch_update_stocks)
        
        stats = sync_result.get_stats()
        logger.info(f"""
Synchronization Analysis Results:
  - Stocks to ADD (new in sources): {stats['to_add']}
  - Stocks to DELETE (removed from sources): {stats['to_delete']}
  - Stocks to REMOVE (due to API errors): {stats['to_remove_due_to_errors']}
  - Stocks to UPDATE (changed data): {stats['to_update']}
  - Stocks UNCHANGED: {stats['unchanged']}
        """)
        
        # 4. Perform synchronization operations
        logger.info("Executing synchronization operations...")
        operation_results = perform_synchronization_operations(stock_repo, sync_result, database_stocks)
        
        # 5. Print final statistics
        print_final_synchronization_statistics(stock_repo, operation_results, sync_result)
    
    except Exception as e:
        logger.error(f"Error during synchronization: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Clean up database connections
        try:
            db_manager.close_all_connections()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing database connections: {e}")

if __name__ == "__main__":
    main()