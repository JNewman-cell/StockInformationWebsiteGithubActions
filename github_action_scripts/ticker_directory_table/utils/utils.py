"""
Utility functions for ticker directory table synchronization.
"""

import logging
import os
import sys
from typing import Dict, List

# Add data layer to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import TickerDirectory
from data_layer.models.ticker_directory import TickerDirectoryStatus
from data_layer.repositories import TickerDirectoryRepository

# Add entities and constants to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from entities.synchronization_result import SynchronizationResult
from constants import BATCH_SIZE

logger = logging.getLogger(__name__)


# ============================================================================
# Ticker Directory Specific Functions
# ============================================================================


def process_tickers_and_build_sync_plan(
    github_tickers_with_ciks: Dict[str, int],
    ticker_directory_repo: TickerDirectoryRepository,
    database_tickers: Dict[str, TickerDirectory],
) -> SynchronizationResult:
    """
    Process GitHub tickers in batches and immediately persist changes to database.
    
    This function:
    1. Processes GitHub ticker-CIK pairs in batches
    2. For each batch, identifies new tickers to add as ACTIVE
    3. Immediately persists additions to database
    4. After all batches, identifies ACTIVE tickers to update to INACTIVE
    5. Immediately persists updates to database in batches
    
    Args:
        github_tickers_with_ciks: Dictionary mapping ticker symbol to CIK
        ticker_directory_repo: Repository for ticker directory operations
        database_tickers: Dictionary of existing tickers in database for comparison (ticker -> TickerDirectory)
        
    Returns:
        SynchronizationResult with operation results
    """
    sync_result = SynchronizationResult()
    
    logger.info("Processing GitHub tickers and persisting immediately...")
    
    # Get set of tickers from GitHub
    github_tickers = set(github_tickers_with_ciks.keys())
    logger.info(f"Processing {len(github_tickers)} tickers from GitHub")
    
    # Process GitHub tickers in batches, adding new entries immediately
    github_tickers_list = list(github_tickers)
    total_batches = (len(github_tickers_list) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(github_tickers_list)} GitHub tickers in {total_batches} batches of {BATCH_SIZE}")
    
    for i in range(0, len(github_tickers_list), BATCH_SIZE):
        batch_tickers = github_tickers_list[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_tickers)} tickers)...")
        
        # Identify new entries in this batch (tickers not in database)
        batch_to_add: List[TickerDirectory] = []
        
        for ticker in batch_tickers:
            if ticker not in database_tickers:
                # New ticker - add it with its CIK
                cik = github_tickers_with_ciks[ticker]
                new_entry = TickerDirectory(
                    ticker=ticker,
                    cik=cik,
                    status=TickerDirectoryStatus.ACTIVE
                )
                batch_to_add.append(new_entry)
        
        # Immediately persist new entries
        if batch_to_add:
            try:
                added_count = ticker_directory_repo.bulk_insert(batch_to_add)
                logger.info(f"Batch {batch_num}: Added {added_count} new ACTIVE entries to database")
                sync_result.to_add.extend(batch_to_add)
                # Update local cache so subsequent operations see these as existing
                for entry in batch_to_add:
                    database_tickers[entry.ticker] = entry
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to add entries: {e}")
                raise
    
    logger.info(f"Completed adding {len(sync_result.to_add)} new ACTIVE entries")
    
    # Now identify ACTIVE entries that should be updated to INACTIVE
    # (tickers in database that are ACTIVE but not in GitHub list)
    logger.info("Identifying ACTIVE entries to update to INACTIVE...")
    
    tickers_to_update: List[str] = []
    for ticker, entry in database_tickers.items():
        if entry.status == TickerDirectoryStatus.ACTIVE:
            if ticker not in github_tickers:
                # Entry is ACTIVE but ticker is not in GitHub list
                tickers_to_update.append(ticker)
            else:
                # Entry is ACTIVE and ticker is in GitHub list - unchanged
                sync_result.unchanged.append(ticker)
        else:
            # Entry is INACTIVE - leave it unchanged
            sync_result.unchanged.append(ticker)
    
    # Update entries to INACTIVE in batches
    if tickers_to_update:
        total_update_batches = (len(tickers_to_update) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Updating {len(tickers_to_update)} ACTIVE entries to INACTIVE in {total_update_batches} batches")
        
        for i in range(0, len(tickers_to_update), BATCH_SIZE):
            batch_tickers = tickers_to_update[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            try:
                # Bulk update status to INACTIVE
                rows_updated = ticker_directory_repo.bulk_update_status(batch_tickers, TickerDirectoryStatus.INACTIVE)
                logger.info(f"Batch {batch_num}/{total_update_batches}: Updated {rows_updated} entries to INACTIVE")
                sync_result.to_update_to_inactive.extend(batch_tickers)
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to update entries: {e}")
                raise
    
    logger.info(f"Completed updating {len(sync_result.to_update_to_inactive)} entries to INACTIVE")
    logger.info(f"Identified {len(sync_result.unchanged)} unchanged entries")
    
    return sync_result
