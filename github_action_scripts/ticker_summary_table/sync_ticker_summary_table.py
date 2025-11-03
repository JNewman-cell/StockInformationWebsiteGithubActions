"""
Ticker Summary table synchronization script.

This script synchronizes ticker summary data with the database using a deterministic approach.
It fetches ticker symbols from the same source as the stocks table, validates them using SEC CIK lookup,
then uses Yahoo Finance API to retrieve summary information. The script performs:
- Validates companies are real by looking up CIK from SEC
- Validates market_cap and previous_close are non-empty and > 0
- Adds new ticker summaries with valid data that are not in the database
- Updates ticker summaries that have changed data
- Removes ticker summaries that fail validation (no CIK, invalid data) or are no longer in sources
- Sets nullable fields (pe_ratio, forward_pe_ratio, dividend_yield, payout_ratio) to NULL when empty
- Uses bulk database operations (bulk_insert, bulk_update, bulk_delete) for efficiency
"""

import logging
import os
import sys
from typing import Dict, Set

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_layer import (
    DatabaseConnectionManager,
    TickerSummaryRepository,
)
from utils.utils import (
    fetch_ticker_data_from_github_repo,
    process_tickers_and_persist_summaries,
    identify_tickers_to_delete,
    delete_obsolete_ticker_summaries,
)
from entities.synchronization_result import SynchronizationResult
from constants import SEC_USER_EMAIL_ENV_VAR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress psycopg connection pool verbose logging that exposes connection strings
logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
logging.getLogger('psycopg').setLevel(logging.ERROR)


def configure_sec_api():
    """Configure the sec-company-lookup package with user email."""
    from sec_company_lookup import set_user_email
    
    user_email = os.getenv(SEC_USER_EMAIL_ENV_VAR)
    if not user_email:
        raise ValueError(f"Environment variable {SEC_USER_EMAIL_ENV_VAR} is not set. "
                        f"This is required for SEC API access.")
    
    set_user_email(user_email)
    logger.info(f"Configured sec-company-lookup with email: {user_email}")


def check_database_connectivity(db_manager: DatabaseConnectionManager, ticker_summary_repo: TickerSummaryRepository) -> bool:
    """
    Check database connectivity and table structure using data layer.
    
    Args:
        db_manager: Database connection manager
        ticker_summary_repo: TickerSummary repository instance
        
    Returns:
        True if database is accessible, False otherwise
    """
    try:
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("✓ Database connection successful")
        
        # Test repository functionality by getting count
        try:
            count = ticker_summary_repo.count()
            logger.info(f"✓ ticker_summary table accessible with {count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ ticker_summary table validation failed: {e}")
            logger.error("Please ensure the ticker_summary table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False


def print_final_synchronization_statistics(
    ticker_summary_repo: TickerSummaryRepository,
    operation_results: Dict[str, int],
    sync_result: SynchronizationResult
):
    """
    Print final synchronization statistics.
    
    Args:
        ticker_summary_repo: TickerSummary repository instance
        operation_results: Dictionary with operation counts (added, updated, deleted)
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
Synchronization Results (with immediate persistence):
  - Added: {operation_results['added']} ticker summaries (persisted immediately during lookup)
  - Updated: {operation_results['updated']} ticker summaries (persisted immediately during lookup)
  - Deleted: {operation_results['deleted']} ticker summaries (bulk delete)
  - Unchanged: {len(sync_result.unchanged)} ticker summaries
  - Failed ticker lookups: {len(sync_result.failed_ticker_lookups)}
  - Failed due to persistent API errors: {len(sync_result.to_remove_due_to_errors)}
    """)
    
    if sync_result.failed_ticker_lookups:
        # Show sample of failed tickers
        sample_size = min(10, len(sync_result.failed_ticker_lookups))
        sample = sync_result.failed_ticker_lookups[:sample_size]
        logger.warning(f"Sample of failed ticker lookups: {', '.join(sample)}"
                      f"{'...' if len(sync_result.failed_ticker_lookups) > sample_size else ''}")
    
    if sync_result.to_remove_due_to_errors:
        # Show sample of tickers removed due to API errors
        sample_size = min(10, len(sync_result.to_remove_due_to_errors))
        sample = sync_result.to_remove_due_to_errors[:sample_size]
        logger.info(f"Sample of tickers removed due to persistent API errors: {', '.join(sample)}"
                   f"{'...' if len(sync_result.to_remove_due_to_errors) > sample_size else ''}")
    
    # Print success summary
    total_operations = (operation_results['added'] + operation_results['deleted'] + 
                       operation_results['updated'])
    print(f"\nSynchronization completed successfully with immediate persistence!")
    print(f"Total operations performed: {total_operations}")
    print(f"  - {operation_results['added']} ticker summaries added (persisted immediately)")
    print(f"  - {operation_results['updated']} ticker summaries updated (persisted immediately)")
    print(f"  - {operation_results['deleted']} ticker summaries deleted")
    print(f"  - {len(sync_result.unchanged)} ticker summaries unchanged")
    print(f"  - {len(sync_result.failed_ticker_lookups)} tickers failed Yahoo Finance lookup")
    print(f"  - {len(sync_result.to_remove_due_to_errors)} tickers removed due to persistent API errors")
    
    # Get final database statistics
    try:
        final_count = ticker_summary_repo.count()
        logger.info(f"Final database state: {final_count} ticker summaries")
        print(f"\nFinal database state: {final_count} total ticker summaries")
    except Exception as e:
        logger.warning(f"Could not retrieve final statistics: {e}")


def main():
    """Main function for ticker summary table synchronization."""
    
    # Configure sec-company-lookup package
    try:
        logger.info("Configuring sec-company-lookup package...")
        configure_sec_api()
    except Exception as e:
        logger.error(f"Failed to configure sec-company-lookup: {e}")
        sys.exit(1)
    
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        ticker_summary_repo = TickerSummaryRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, ticker_summary_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        logger.info("=== Starting Ticker Summary Table Synchronization ===")
        
        # 1. Fetch ticker data from GitHub repository (same source as stocks table)
        logger.info("Fetching ticker data from GitHub repository...")
        ticker_symbols = fetch_ticker_data_from_github_repo()
        logger.info(f"Loaded {len(ticker_symbols)} ticker symbols from GitHub repository")
        
        # 2. Get current database state
        logger.info("Retrieving current database state...")
        database_ticker_summary_list = ticker_summary_repo.get_all()
        database_ticker_summaries = {ts.ticker: ts for ts in database_ticker_summary_list}
        logger.info(f"Found {len(database_ticker_summaries)} ticker summaries currently in database")
        
        # 3. Process tickers in batches: lookup summary data and persist immediately
        # This is the key improvement - data is saved incrementally as it's retrieved
        logger.info("Processing tickers and persisting ticker summaries immediately...")
        sync_result = process_tickers_and_persist_summaries(ticker_symbols, ticker_summary_repo, database_ticker_summaries)
        
        stats = sync_result.get_stats()
        logger.info(f"""
Lookup and Persistence Results:
  - Ticker Summaries ADDED (new in sources): {stats['to_add']}
  - Ticker Summaries UPDATED (changed data): {stats['to_update']}
  - Ticker Summaries UNCHANGED: {stats['unchanged']}
  - Failed ticker lookups: {stats['failed_ticker_lookups']}
  - To remove due to persistent API errors: {stats['to_remove_due_to_errors']}
        """)
        
        # 4. Identify and delete ticker summaries that fail validation or are no longer in source data
        # This includes:
        # - Tickers no longer in the GitHub source
        # - Tickers that failed CIK lookup (not real companies)
        # - Tickers that failed Yahoo Finance lookup
        # - Tickers with invalid market_cap or previous_close (empty/zero)
        logger.info("Identifying ticker summaries to delete...")
        processed_tickers: Set[str] = set()
        for ticker_summary in sync_result.to_add:
            processed_tickers.add(ticker_summary.ticker)
        for ticker_summary in sync_result.to_update:
            processed_tickers.add(ticker_summary.ticker)
        processed_tickers.update(sync_result.unchanged)
        
        tickers_to_delete = identify_tickers_to_delete(database_ticker_summaries, processed_tickers)
        
        # Add tickers that failed validation checks (CIK lookup, Yahoo lookup, or invalid data)
        if sync_result.to_remove_due_to_errors:
            logger.info(f"Adding {len(sync_result.to_remove_due_to_errors)} tickers that failed validation checks to delete list")
            tickers_to_delete.extend(sync_result.to_remove_due_to_errors)
            # Remove duplicates
            tickers_to_delete = list(set(tickers_to_delete))
        
        deleted_count = 0
        if tickers_to_delete:
            logger.info(f"Deleting {len(tickers_to_delete)} obsolete/problematic ticker summaries...")
            deleted_count = delete_obsolete_ticker_summaries(ticker_summary_repo, tickers_to_delete)
        else:
            logger.info("No obsolete ticker summaries to delete")
        
        # 5. Print final statistics
        final_stats = {
            'added': len(sync_result.to_add),
            'updated': len(sync_result.to_update),
            'deleted': deleted_count
        }
        print_final_synchronization_statistics(ticker_summary_repo, final_stats, sync_result)
    
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
