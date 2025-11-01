"""
CIK lookup synchronization transformer functions.

This module contains transformation and comparison logic for CIK lookup synchronization.
"""

import logging
import os
import sys
from typing import Dict, List, Set
from datetime import datetime, timezone

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import CikLookup
from entities.synchronization_result import SynchronizationResult

logger = logging.getLogger(__name__)


def analyze_database_vs_source_ciks_for_synchronization_operations(
    database_ciks: Dict[int, CikLookup],
    source_ciks: Dict[int, CikLookup],
    update_batch_func=None
) -> SynchronizationResult:
    """
    Compare database CIKs with source CIKs to determine sync operations.

    This function implements comprehensive synchronization logic:
    - Source CIKs not in database -> add to database
    - Database CIKs not in source -> delete from database
    - CIKs in both -> check for company name changes and update immediately if needed

    Args:
        database_ciks: Dictionary mapping CIK to CikLookup object from database
        source_ciks: Dictionary mapping CIK to CikLookup object from source data
        update_batch_func: Optional function to immediately update batches of CIK lookups

    Returns:
        SynchronizationResult object containing all operations to perform
    """
    result = SynchronizationResult()

    # Create sets for efficient comparison
    db_cik_numbers = set(database_ciks.keys())
    source_cik_numbers = set(source_ciks.keys())

    # Find CIKs to add (in source but not in database)
    ciks_to_add = source_cik_numbers - db_cik_numbers
    for cik in ciks_to_add:
        result.to_add.append(source_ciks[cik])

    # Find CIKs to delete (in database but not in source)
    ciks_to_delete = db_cik_numbers - source_cik_numbers
    result.to_delete.extend(ciks_to_delete)

    # Find CIKs that exist in both - check for company name changes
    common_ciks = db_cik_numbers & source_cik_numbers

    if common_ciks:
        logger.info(f"Checking {len(common_ciks)} existing CIKs for company name changes")

        # Collect CIKs that need updates
        ciks_needing_updates = []

        for cik in common_ciks:
            db_cik_lookup = database_ciks[cik]
            source_cik_lookup = source_ciks[cik]

            # Check if company name changed
            if db_cik_lookup.company_name != source_cik_lookup.company_name:
                # Create updated CIK lookup with preserved timestamps
                updated_cik_lookup = CikLookup(
                    cik=cik,
                    company_name=source_cik_lookup.company_name,
                    created_at=db_cik_lookup.created_at,
                    last_updated_at=datetime.now(timezone.utc).replace(tzinfo=None)
                )
                ciks_needing_updates.append(updated_cik_lookup)
            else:
                # CIK is unchanged
                result.unchanged.append(cik)

        # Process updates in batches if update function provided
        if ciks_needing_updates:
            if update_batch_func:
                logger.info(f"Immediately processing {len(ciks_needing_updates)} CIK updates in batches")
                
                # Process in batches of 500 for efficiency
                batch_size = 500
                total_updated = 0
                
                for i in range(0, len(ciks_needing_updates), batch_size):
                    batch = ciks_needing_updates[i:i + batch_size]
                    try:
                        updated_count = update_batch_func(batch)
                        total_updated += updated_count
                        logger.info(f"Batch {i // batch_size + 1}: Updated {updated_count} CIKs")
                    except Exception as e:
                        logger.warning(f"Failed to process batch {i // batch_size + 1}: {e}")
                        # Fall back to adding failed batch to result for later processing
                        result.to_update.extend(batch)
                
                logger.info(f"Successfully processed {total_updated} CIK updates immediately during analysis")
            else:
                # No batch function - add to result for later processing
                result.to_update.extend(ciks_needing_updates)

    logger.info(f"Analysis complete: {len(result.to_add)} to add, {len(result.to_delete)} to delete, "
                f"{len(result.to_update)} to update, {len(result.unchanged)} unchanged")

    return result