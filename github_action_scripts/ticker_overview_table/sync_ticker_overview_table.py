"""
Ticker Overview table synchronization script.

This script synchronizes ticker overview data with the database using a deterministic approach.
It fetches ticker symbols from the ticker_summary table (which have already been validated),
then uses Yahoo Finance API to retrieve overview information from key_stats and financial_data modules.
The script performs:
- Fetches tickers from ticker_summary table (already validated with CIK and market data)
- Retrieves overview data from Yahoo Finance (key_stats and financial_data modules)
- Converts margins and growth rates from 0.XXXX to XX.XX percentage format
- Adds new ticker overviews with valid data that are not in the database
- Updates ticker overviews that have changed data
- Uses bulk database operations (bulk_insert, bulk_update) for efficiency
- Deletions are handled by the ticker_summary synchronization to maintain referential integrity
"""

import logging
import os
import sys
from typing import Dict

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_layer import (
    DatabaseConnectionManager,
    TickerOverviewRepository,
    TickerSummaryRepository,
)
from utils.utils import (
    process_tickers_and_persist_overviews,
)
from entities.synchronization_result import SynchronizationResult
from yahooquery.session_management import initialize_session  # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress psycopg connection pool verbose logging that exposes connection strings
logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
logging.getLogger('psycopg').setLevel(logging.ERROR)


def check_database_connectivity(
    db_manager: DatabaseConnectionManager,
    ticker_overview_repo: TickerOverviewRepository,
    ticker_summary_repo: TickerSummaryRepository
) -> bool:
    """
    Check database connectivity and table structure using data layer.
    
    Args:
        db_manager: Database connection manager
        ticker_overview_repo: TickerOverview repository instance
        ticker_summary_repo: TickerSummary repository instance
        
    Returns:
        True if database is accessible, False otherwise
    """
    try:
        if not db_manager.test_connection():
            logger.error("Database connection test failed")
            return False
        
        logger.info("✓ Database connection successful")
        
        # Test ticker_summary table (must exist as it's the source)
        try:
            summary_count = ticker_summary_repo.count()
            logger.info(f"✓ ticker_summary table accessible with {summary_count} records")
            
            if summary_count == 0:
                logger.warning("⚠ ticker_summary table is empty - no tickers to process")
        except Exception as e:
            logger.error(f"✗ ticker_summary table validation failed: {e}")
            logger.error("Please ensure the ticker_summary table exists and is populated")
            return False
        
        # Test ticker_overview table
        try:
            overview_count = ticker_overview_repo.count()
            logger.info(f"✓ ticker_overview table accessible with {overview_count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ ticker_overview table validation failed: {e}")
            logger.error("Please ensure the ticker_overview table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False


def print_final_synchronization_statistics(
    ticker_overview_repo: TickerOverviewRepository,
    operation_results: Dict[str, int],
    sync_result: SynchronizationResult
):
    """
    Print final synchronization statistics.
    
    Args:
        ticker_overview_repo: TickerOverview repository instance
        operation_results: Dictionary with operation counts (added, updated, deleted)
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
        Synchronization Results (with immediate persistence):
        - Added: {operation_results['added']} ticker overviews (persisted immediately during lookup)
        - Updated: {operation_results['updated']} ticker overviews (persisted immediately during lookup)
        - Deleted: {operation_results['deleted']} ticker overviews (deletions handled by ticker_summary sync)
        - Unchanged: {len(sync_result.unchanged)} ticker overviews
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
    print(f"  - {operation_results['added']} ticker overviews added (persisted immediately)")
    print(f"  - {operation_results['updated']} ticker overviews updated (persisted immediately)")
    print(f"  - {operation_results['deleted']} ticker overviews deleted (handled by ticker_summary sync)")
    print(f"  - {len(sync_result.unchanged)} ticker overviews unchanged")
    print(f"  - {len(sync_result.failed_ticker_lookups)} tickers failed Yahoo Finance lookup")
    print(f"  - {len(sync_result.to_remove_due_to_errors)} tickers removed due to persistent API errors")


def main():
    """Main function for ticker overview table synchronization."""
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        # Use minimal connection pool for GitHub Actions (short-lived, single-threaded)
        db_manager = DatabaseConnectionManager(min_connections=1, max_connections=1)
        ticker_overview_repo = TickerOverviewRepository(db_manager)
        ticker_summary_repo = TickerSummaryRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, ticker_overview_repo, ticker_summary_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        logger.info("=== Starting Ticker Overview Table Synchronization ===")
        
        # 1. Fetch ticker symbols from ticker_summary table (already validated)
        logger.info("Fetching ticker symbols from ticker_summary table...")
        ticker_summaries = ticker_summary_repo.get_all()
        ticker_symbols = [ts.ticker for ts in ticker_summaries]
        logger.info(f"Loaded {len(ticker_symbols)} ticker symbols from ticker_summary table")
        
        if not ticker_symbols:
            logger.warning("No tickers found in ticker_summary table. Nothing to process.")
            sys.exit(0)
        
        # 2. Get current database state
        logger.info("Retrieving current database state...")
        database_ticker_overview_list = ticker_overview_repo.get_all()
        database_ticker_overviews = {to.ticker: to for to in database_ticker_overview_list}
        logger.info(f"Found {len(database_ticker_overviews)} ticker overviews currently in database")
        
        # 3. Create a single asynchronous user-managed session and reuse across batches
        logger.info("Initializing single async yahooquery session for this synchronization transaction...")
        s = initialize_session(None, asynchronous=True)  # type: ignore

        # 4. Process tickers in batches: lookup overview data and persist immediately
        logger.info("Processing tickers and persisting ticker overviews immediately...")
        sync_result = process_tickers_and_persist_overviews(
            ticker_symbols,
            ticker_overview_repo,
            database_ticker_overviews,
            session=s,  # type: ignore
        )
        
        stats = sync_result.get_stats()
        logger.info(f"""
            Lookup and Persistence Results:
            - Ticker Overviews ADDED (new tickers): {stats['to_add']}
            - Ticker Overviews UPDATED (changed data): {stats['to_update']}
            - Ticker Overviews UNCHANGED: {stats['unchanged']}
            - Failed ticker lookups: {stats['failed_ticker_lookups']}
            - To remove due to persistent API errors: {stats['to_remove_due_to_errors']}
        """)
        
        # 5. Print final statistics
        final_stats = {
            'added': len(sync_result.to_add),
            'updated': len(sync_result.to_update),
            'deleted': 0  # No deletions performed in ticker overview sync
        }
        print_final_synchronization_statistics(ticker_overview_repo, final_stats, sync_result)
    
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
