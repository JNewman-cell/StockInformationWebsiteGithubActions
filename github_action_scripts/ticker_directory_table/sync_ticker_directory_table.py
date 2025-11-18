"""
Ticker Directory table synchronization script.

This script synchronizes ticker directory data with the database using a deterministic approach.
It fetches ticker symbols from GitHub repository, looks up their CIKs, then performs:
- Adds new CIK entries as ACTIVE if not in the database
- Updates ACTIVE entries to INACTIVE if ticker is no longer in GitHub list
- Updates timestamps only on creation and status changes
- Processes updates in batches of 50
"""

import logging
import os
import sys
from typing import Dict

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
# Ensure github_action_scripts package (contains shared utils) is on path so
# `from utils.utils import ...` resolves to github_action_scripts/utils, not
# a top-level utils package.
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data_layer import (
    DatabaseConnectionManager,
)
from data_layer.repositories import TickerDirectoryRepository
from github_action_scripts.utils.utils import (
    fetch_ticker_data_from_github_repo,
    lookup_cik_batch,
)
from utils.utils import (
    process_tickers_and_build_sync_plan,
)
from entities.synchronization_result import SynchronizationResult

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress psycopg connection pool verbose logging that exposes connection strings
logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
logging.getLogger('psycopg').setLevel(logging.ERROR)


def check_database_connectivity(db_manager: DatabaseConnectionManager, ticker_directory_repo: TickerDirectoryRepository) -> bool:
    """
    Check database connectivity and table structure using data layer.
    
    Args:
        db_manager: Database connection manager
        ticker_directory_repo: TickerDirectory repository instance
        
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
            count = ticker_directory_repo.count()
            logger.info(f"✓ ticker_directory table accessible with {count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ ticker_directory table validation failed: {e}")
            logger.error("Please ensure the ticker_directory table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False


def print_final_synchronization_statistics(
    ticker_directory_repo: TickerDirectoryRepository,
    operation_results: Dict[str, int],
    sync_result: SynchronizationResult
):
    """
    Print final synchronization statistics.
    
    Args:
        ticker_directory_repo: TickerDirectory repository instance
        operation_results: Dictionary with operation counts (added, updated)
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
Synchronization Results:
  - Added: {operation_results['added']} ticker directory entries (ACTIVE)
  - Updated to INACTIVE: {operation_results['updated']} ticker directory entries
  - Unchanged: {len(sync_result.unchanged)} ticker directory entries
  - Failed ticker lookups: {len(sync_result.failed_ticker_lookups)}
    """)
    
    if sync_result.failed_ticker_lookups:
        # Show sample of failed tickers
        sample_size = min(10, len(sync_result.failed_ticker_lookups))
        sample = sync_result.failed_ticker_lookups[:sample_size]
        logger.warning(f"Sample of failed ticker lookups: {', '.join(sample)}"
                      f"{'...' if len(sync_result.failed_ticker_lookups) > sample_size else ''}")
    
    # Print success summary
    total_operations = operation_results['added'] + operation_results['updated']
    print(f"\nSynchronization completed successfully!")
    print(f"Total operations performed: {total_operations}")
    print(f"  - {operation_results['added']} ticker directory entries added as ACTIVE")
    print(f"  - {operation_results['updated']} ticker directory entries updated to INACTIVE")
    print(f"  - {len(sync_result.unchanged)} ticker directory entries unchanged")
    print(f"  - {len(sync_result.failed_ticker_lookups)} tickers failed CIK lookup")
    
    # Get final database statistics
    try:
        final_count = ticker_directory_repo.count()
        logger.info(f"Final database state: {final_count} ticker directory entries")
        print(f"\nFinal database state: {final_count} total ticker directory entries")
    except Exception as e:
        logger.warning(f"Could not retrieve final statistics: {e}")


def main():
    """Main function for ticker directory table synchronization."""
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        ticker_directory_repo = TickerDirectoryRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, ticker_directory_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        logger.info("=== Starting Ticker Directory Table Synchronization ===")
        
        # 1. Fetch ticker data from GitHub repository
        logger.info("Fetching ticker data from GitHub repository...")
        ticker_symbols = fetch_ticker_data_from_github_repo()
        logger.info(f"Loaded {len(ticker_symbols)} ticker symbols from GitHub repository")
        
        # 2. Lookup CIKs for all tickers
        logger.info("Looking up CIKs for tickers...")
        ticker_to_cik_map, failed_tickers = lookup_cik_batch(ticker_symbols)
        logger.info(f"Successfully mapped {len(ticker_to_cik_map)} tickers to CIKs")
        
        if failed_tickers:
            logger.warning(f"{len(failed_tickers)} tickers failed CIK lookup")
        
        # 3. Get current database state (all entries)
        logger.info("Retrieving current database state...")
        database_entries = ticker_directory_repo.get_all()
        database_tickers = {entry.ticker: entry for entry in database_entries if entry.ticker}
        logger.info(f"Found {len(database_tickers)} ticker directory entries currently in database")
        
        # 4. Process GitHub tickers in batches and persist immediately
        logger.info("Processing GitHub tickers and persisting changes immediately...")
        sync_result = process_tickers_and_build_sync_plan(
            ticker_to_cik_map,
            ticker_directory_repo,
            database_tickers
        )
        sync_result.failed_ticker_lookups = failed_tickers
        
        stats = sync_result.get_stats()
        logger.info(f"""
Synchronization Results (with immediate persistence):
  - ADDED (ACTIVE): {stats['to_add']}
  - UPDATED (to INACTIVE): {stats['to_update_to_inactive']}
  - UNCHANGED: {stats['unchanged']}
  - Failed ticker lookups: {stats['failed_ticker_lookups']}
        """)
        
        # 5. Print final statistics
        operation_results = {
            'added': len(sync_result.to_add),
            'updated': len(sync_result.to_update_to_inactive)
        }
        print_final_synchronization_statistics(
            ticker_directory_repo,
            operation_results,
            sync_result
        )
    
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
