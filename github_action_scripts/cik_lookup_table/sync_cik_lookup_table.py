"""
CIK Lookup table synchronization script.

This script synchronizes CIK lookup data with the database using a deterministic approach.
It fetches ticker symbols from the same source as the stocks table, then uses sec-company-lookup
to retrieve CIK and company name information. The script performs:
- Adds new CIK entries from sources that are not in the database
- Removes CIK entries from database that are no longer in sources
- Updates CIK entries that have changed company names
"""

import logging
import os
import sys
from typing import Dict, Set

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
# Ensure github_action_scripts package (contains shared utils) is on path so
# `from utils.utils import ...` resolves to github_action_scripts/utils, not
# a top-level utils package.
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from data_layer import (
    DatabaseConnectionManager,
)
from data_layer.repositories import CikLookupRepository, TickerSummaryRepository
from github_action_scripts.utils.utils import fetch_ticker_data_from_github_repo
from utils.utils import (
    process_tickers_and_persist_ciks,
    identify_ciks_to_delete,
    delete_obsolete_ciks,
)
from entities.synchronization_result import SynchronizationResult

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress psycopg connection pool verbose logging that exposes connection strings
logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
logging.getLogger('psycopg').setLevel(logging.ERROR)

def check_database_connectivity(db_manager: DatabaseConnectionManager, cik_repo: CikLookupRepository) -> bool:
    """
    Check database connectivity and table structure using data layer.
    
    Args:
        db_manager: Database connection manager
        cik_repo: CIK lookup repository instance
        
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
            count = cik_repo.count()
            logger.info(f"✓ cik_lookup table accessible with {count} existing records")
            return True
        except Exception as e:
            logger.error(f"✗ cik_lookup table validation failed: {e}")
            logger.error("Please ensure the cik_lookup table exists with the required schema")
            return False
            
    except Exception as e:
        logger.error(f"Database connectivity check failed: {e}")
        return False


def print_final_synchronization_statistics(
    cik_repo: CikLookupRepository,
    operation_results: Dict[str, int],
    sync_result: SynchronizationResult
):
    """
    Print final synchronization statistics.
    
    Args:
        cik_repo: CIK lookup repository instance
        operation_results: Dictionary with operation counts (added, updated, deleted)
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
Synchronization Results (with immediate persistence):
  - Added: {operation_results['added']} CIK entries (persisted immediately during lookup)
  - Updated: {operation_results['updated']} CIK entries (persisted immediately during lookup)
  - Deleted: {operation_results['deleted']} CIK entries (bulk delete)
  - Unchanged: {len(sync_result.unchanged)} CIK entries
  - Failed ticker lookups: {len(sync_result.failed_ticker_lookups)}
    """)
    
    if sync_result.failed_ticker_lookups:
        # Show sample of failed tickers
        sample_size = min(10, len(sync_result.failed_ticker_lookups))
        sample = sync_result.failed_ticker_lookups[:sample_size]
        logger.warning(f"Sample of failed ticker lookups: {', '.join(sample)}"
                      f"{'...' if len(sync_result.failed_ticker_lookups) > sample_size else ''}")
    
    # Print success summary
    total_operations = (operation_results['added'] + operation_results['deleted'] + 
                       operation_results['updated'])
    print(f"\nSynchronization completed successfully with immediate persistence!")
    print(f"Total operations performed: {total_operations}")
    print(f"  - {operation_results['added']} CIK entries added (persisted immediately)")
    print(f"  - {operation_results['updated']} CIK entries updated (persisted immediately)")
    print(f"  - {operation_results['deleted']} CIK entries deleted")
    print(f"  - {len(sync_result.unchanged)} CIK entries unchanged")
    print(f"  - {len(sync_result.failed_ticker_lookups)} tickers failed CIK lookup")
    
    # Get final database statistics
    try:
        final_count = cik_repo.count()
        logger.info(f"Final database state: {final_count} CIK entries")
        print(f"\nFinal database state: {final_count} total CIK entries")
    except Exception as e:
        logger.warning(f"Could not retrieve final statistics: {e}")


def main():
    """Main function for CIK lookup table synchronization."""
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        cik_repo = CikLookupRepository(db_manager)
        ticker_summary_repo = TickerSummaryRepository(db_manager)
        
        # Check database connectivity and table structure
        if not check_database_connectivity(db_manager, cik_repo):
            logger.error("Cannot proceed without proper database setup.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to initialize data layer: {e}")
        sys.exit(1)
    
    try:
        logger.info("=== Starting CIK Lookup Table Synchronization ===")
        
        # 1. Fetch ticker data from GitHub repository (same source as stocks table)
        logger.info("Fetching ticker data from GitHub repository...")
        ticker_symbols = fetch_ticker_data_from_github_repo()
        logger.info(f"Loaded {len(ticker_symbols)} ticker symbols from GitHub repository")
        
        # 2. Get current database state
        logger.info("Retrieving current database state...")
        database_cik_list = cik_repo.get_all()
        database_ciks = {cik_lookup.cik: cik_lookup for cik_lookup in database_cik_list}
        logger.info(f"Found {len(database_ciks)} CIK entries currently in database")
        
        # 3. Process tickers in batches: lookup CIKs and persist immediately
        # This is the key improvement - data is saved incrementally as it's retrieved
        logger.info("Processing tickers and persisting CIKs immediately...")
        sync_result = process_tickers_and_persist_ciks(ticker_symbols, cik_repo, database_ciks)
        
        stats = sync_result.get_stats()
        logger.info(f"""
            Lookup and Persistence Results:
            - CIKs ADDED (new in sources): {stats['to_add']}
            - CIKs UPDATED (changed company names): {stats['to_update']}
            - CIKs UNCHANGED: {stats['unchanged']}
            - Failed ticker lookups: {stats['failed_ticker_lookups']}
        """)
        
        # 4. Identify and delete CIKs that are no longer in source data
        logger.info("Identifying obsolete CIKs...")
        processed_ciks: Set[int] = set()
        for cik_lookup in sync_result.to_add:
            processed_ciks.add(cik_lookup.cik)
        for cik_lookup in sync_result.to_update:
            processed_ciks.add(cik_lookup.cik)
        processed_ciks.update(sync_result.unchanged)
        
        ciks_to_delete = identify_ciks_to_delete(database_ciks, processed_ciks)
        
        if ciks_to_delete:
            logger.info(f"Deleting {len(ciks_to_delete)} obsolete CIKs...")
            deleted_count = delete_obsolete_ciks(cik_repo, ticker_summary_repo, ciks_to_delete)
            sync_result.to_delete = ciks_to_delete
        else:
            logger.info("No obsolete CIKs to delete")
            deleted_count = 0
        
        # 5. Print final statistics
        final_stats = {
            'added': len(sync_result.to_add),
            'updated': len(sync_result.to_update),
            'deleted': deleted_count
        }
        print_final_synchronization_statistics(cik_repo, final_stats, sync_result)
    
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
