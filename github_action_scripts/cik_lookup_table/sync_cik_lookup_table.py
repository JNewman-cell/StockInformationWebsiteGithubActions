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
from typing import Dict

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_layer import (
    DatabaseConnectionManager,
)
from data_layer.repositories import CikLookupRepository
from utils.utils import (
    fetch_ticker_data_from_github_repo,
    process_tickers_in_batches,
    create_cik_lookup_entities,
    analyze_synchronization_operations,
)
from entities.synchronization_result import SynchronizationResult

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def configure_sec_company_lookup():
    """Configure the sec-company-lookup package with user email."""
    from sec_company_lookup import set_user_email
    
    # Try to get email from environment variable
    user_email = 'jpnewman167@gmail.com'
    
    set_user_email(user_email)
    logger.info(f"Configured sec-company-lookup with email: {user_email}")


def perform_synchronization_operations(
    cik_repo: CikLookupRepository,
    sync_result: SynchronizationResult
) -> Dict[str, int]:
    """
    Execute the synchronization operations.
    
    Args:
        cik_repo: CIK lookup repository instance
        sync_result: SynchronizationResult containing operations to perform
        
    Returns:
        Dictionary with operation counts
    """
    results = {
        'added': 0,
        'deleted': 0,
        'updated': 0
    }
    
    # 1. Delete CIKs that are no longer in the source (bulk operation)
    if sync_result.to_delete:
        logger.info(f"Bulk deleting {len(sync_result.to_delete)} CIK entries no longer in source data")
        deleted_count = 0
        failed_count = 0
        
        for cik in sync_result.to_delete:
            try:
                if cik_repo.delete(cik):
                    deleted_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.warning(f"Failed to delete CIK {cik}: {e}")
                failed_count += 1
        
        results['deleted'] = deleted_count
        
        if deleted_count > 0:
            logger.info(f"Successfully deleted {deleted_count} CIK entries no longer in source")
        if failed_count > 0:
            logger.warning(f"Failed to delete {failed_count} CIK entries (not found in database)")
    
    # 1.1. Remove CIKs that have persistent errors (if any)
    if sync_result.to_remove_due_to_errors:
        logger.info(f"Bulk removing {len(sync_result.to_remove_due_to_errors)} CIK entries due to persistent errors")
        deleted_count = 0
        failed_count = 0
        
        for cik in sync_result.to_remove_due_to_errors:
            try:
                if cik_repo.delete(cik):
                    deleted_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.warning(f"Failed to remove CIK {cik}: {e}")
                failed_count += 1
        
        results['deleted'] += deleted_count
        
        if deleted_count > 0:
            logger.info(f"Successfully removed {deleted_count} CIK entries due to errors")
        if failed_count > 0:
            logger.warning(f"Failed to remove {failed_count} CIK entries with errors (not found in database)")
    
    # 2. Add new CIK entries
    if sync_result.to_add:
        logger.info(f"Adding {len(sync_result.to_add)} new CIK entries")
        
        # Use bulk upsert for efficiency (handles conflicts gracefully)
        try:
            affected = cik_repo.bulk_upsert(sync_result.to_add)
            results['added'] = affected
            logger.info(f"Successfully added {affected} new CIK entries using bulk upsert")
        except Exception as e:
            logger.error(f"Bulk upsert failed: {e}")
            # Fallback to individual inserts
            logger.info("Falling back to individual inserts...")
            for cik_lookup in sync_result.to_add:
                try:
                    cik_repo.create(cik_lookup)
                    results['added'] += 1
                except Exception as insert_error:
                    logger.warning(f"Failed to add CIK {cik_lookup.cik}: {insert_error}")
    
    # 3. Update existing CIK entries with changed company names
    if sync_result.to_update:
        logger.info(f"Updating {len(sync_result.to_update)} CIK entries with changed company names")
        
        # Use bulk upsert which handles updates efficiently
        try:
            affected = cik_repo.bulk_upsert(sync_result.to_update)
            results['updated'] = affected
            logger.info(f"Successfully updated {affected} CIK entries using bulk upsert")
        except Exception as e:
            logger.error(f"Bulk update failed: {e}")
            # Fallback to individual updates
            logger.info("Falling back to individual updates...")
            for cik_lookup in sync_result.to_update:
                try:
                    cik_repo.update(cik_lookup)
                    results['updated'] += 1
                except Exception as update_error:
                    logger.warning(f"Failed to update CIK {cik_lookup.cik}: {update_error}")
    
    logger.info(f"Synchronization operations complete: {results}")
    return results


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
        operation_results: Dictionary with operation counts
        sync_result: SynchronizationResult with details
    """
    logger.info("=== Synchronization Complete ===")
    
    # Print operation results
    logger.info(f"""
Bulk Operation Results:
  - Added: {operation_results['added']} CIK entries (via bulk upsert)
  - Deleted: {operation_results['deleted']} CIK entries (via bulk delete)
  - Updated: {operation_results['updated']} CIK entries (via bulk upsert)
  - Failed ticker lookups: {len(sync_result.failed_ticker_lookups)}
    """)
    
    if sync_result.failed_ticker_lookups:
        # Show sample of failed tickers
        sample_size = min(10, len(sync_result.failed_ticker_lookups))
        sample = sync_result.failed_ticker_lookups[:sample_size]
        logger.warning(f"Sample of failed ticker lookups: {', '.join(sample)}"
                      f"{'...' if len(sync_result.failed_ticker_lookups) > sample_size else ''}")
    
    if sync_result.to_remove_due_to_errors:
        logger.info(f"Removed CIK entries due to persistent errors: {', '.join(map(str, sync_result.to_remove_due_to_errors[:10]))}"
                   f"{'...' if len(sync_result.to_remove_due_to_errors) > 10 else ''}")
    
    # Print success summary
    total_operations = (operation_results['added'] + operation_results['deleted'] + 
                       operation_results['updated'])
    print(f"\nSynchronization completed successfully using optimized bulk operations!")
    print(f"Total operations performed: {total_operations}")
    print(f"  - {operation_results['added']} CIK entries added (bulk upsert)")
    print(f"  - {operation_results['deleted']} CIK entries deleted (bulk delete)") 
    print(f"  - {operation_results['updated']} CIK entries updated (bulk upsert)")
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
    
    # Configure sec-company-lookup package
    try:
        logger.info("Configuring sec-company-lookup package...")
        configure_sec_company_lookup()
    except Exception as e:
        logger.error(f"Failed to configure sec-company-lookup: {e}")
        sys.exit(1)
    
    # Initialize data layer components
    try:
        logger.info("Initializing data layer...")
        db_manager = DatabaseConnectionManager()  # Uses DATABASE_URL from environment
        cik_repo = CikLookupRepository(db_manager)
        
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
        
        # 2. Lookup CIK and company names for all tickers using sec-company-lookup
        logger.info("Looking up CIK and company names using sec-company-lookup...")
        ticker_cik_map, failed_tickers = process_tickers_in_batches(ticker_symbols, batch_size=100)
        logger.info(f"Successfully looked up {len(ticker_cik_map)} tickers")
        if failed_tickers:
            logger.warning(f"{len(failed_tickers)} tickers failed CIK lookup")
        
        # 3. Create CikLookup entities (grouped by CIK)
        logger.info("Creating CikLookup entities...")
        source_ciks = create_cik_lookup_entities(ticker_cik_map)
        logger.info(f"Created {len(source_ciks)} unique CIK entries")
        
        # 4. Get current database state
        logger.info("Retrieving current database state...")
        database_cik_list = cik_repo.get_all()
        database_ciks = {cik_lookup.cik: cik_lookup for cik_lookup in database_cik_list}
        logger.info(f"Found {len(database_ciks)} CIK entries currently in database")
        
        # 5. Analyze differences and determine operations
        logger.info("Analyzing differences between source and database...")
        sync_result = analyze_synchronization_operations(database_ciks, source_ciks)
        
        # Store failed ticker lookups in sync_result
        sync_result.failed_ticker_lookups = failed_tickers
        
        stats = sync_result.get_stats()
        logger.info(f"""
Synchronization Analysis Results:
  - CIKs to ADD (new in sources): {stats['to_add']}
  - CIKs to DELETE (removed from sources): {stats['to_delete']}
  - CIKs to REMOVE (due to errors): {stats['to_remove_due_to_errors']}
  - CIKs to UPDATE (changed company names): {stats['to_update']}
  - CIKs UNCHANGED: {stats['unchanged']}
        """)
        
        # 6. Perform synchronization operations
        logger.info("Executing synchronization operations...")
        operation_results = perform_synchronization_operations(cik_repo, sync_result)
        
        # 7. Print final statistics
        print_final_synchronization_statistics(cik_repo, operation_results, sync_result)
    
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
