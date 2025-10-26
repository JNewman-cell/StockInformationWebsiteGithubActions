"""
Stock synchronization transformer functions.

This module contains transformation and comparison logic for stock synchronization.
"""

import logging
import os
import sys
from typing import Dict, List, Set, Tuple
from datetime import datetime, timezone

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer import Stock
from entities.synchronization_result import SynchronizationResult

logger = logging.getLogger(__name__)


def analyze_database_vs_source_symbols_for_synchronization_operations(
    database_stocks: Dict[str, Stock], 
    source_symbols: Set[Tuple[str, str]],
    update_batch_func=None
) -> SynchronizationResult:
    """
    Compare database stocks with source symbols and Yahoo Finance data to determine sync operations.
    
    This function implements comprehensive synchronization logic:
    - Source symbols not in database -> add to database (after validation)
    - Database symbols not in source -> delete from database  
    - Symbols in both -> batch verify against Yahoo Finance API for complete accuracy
    
    Args:
        database_stocks: Dictionary mapping symbol to Stock object from database
        source_symbols: Set of symbol strings from data sources
        update_batch_func: Optional function to immediately update batches of stocks
        
    Returns:
        SynchronizationResult object containing all operations to perform
    """
    result = SynchronizationResult()
    
    # Create sets for efficient comparison
    db_symbols = set(database_stocks.keys())
    source_symbol_names = set(source_symbols)  # source_symbols is now a set of strings
    
    # Find symbols to add (in source but not in database)
    symbols_to_add = source_symbol_names - db_symbols
    for symbol in symbols_to_add:
        result.to_add.append(symbol)
    
    # Find symbols to delete (in database but not in source)
    symbols_to_delete = db_symbols - source_symbol_names
    result.to_delete.extend(symbols_to_delete)
    
    # Find symbols that exist in both - ALL need verification against Yahoo Finance
    common_symbols = db_symbols & source_symbol_names
    
    if common_symbols:
        logger.info(f"Batch verifying {len(common_symbols)} existing stocks against Yahoo Finance API for complete accuracy")
        
        # Get all common stocks for batch verification
        common_stocks = [database_stocks[symbol] for symbol in common_symbols]
        
        # Import the Yahoo Finance verification function
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from utils.utils import compare_existing_stocks_with_yahoo_finance_data_for_updates
        
        # Batch check all common stocks against Yahoo Finance
        stocks_needing_updates, yahoo_failures = compare_existing_stocks_with_yahoo_finance_data_for_updates(common_stocks, update_batch_func)
        
        # Create a set of symbols that need Yahoo-based updates
        yahoo_update_symbols = {stock.symbol for stock in stocks_needing_updates}
        
        # Process each common symbol
        for symbol in common_symbols:
            db_stock = database_stocks[symbol]
            
            # Check if this stock needs updates from Yahoo data differences
            needs_update = False
            
            # Start with current stock data
            updated_stock = Stock(
                symbol=db_stock.symbol,
                company=db_stock.company,
                created_at=db_stock.created_at,
                last_updated_at=db_stock.last_updated_at
            )
            
            # Check if Yahoo Finance data differs
            if symbol in yahoo_update_symbols:
                # Find the updated stock object from Yahoo verification
                yahoo_updated_stock = next((s for s in stocks_needing_updates if s.symbol == symbol), None)
                if yahoo_updated_stock:
                    updated_stock.company = yahoo_updated_stock.company
                    updated_stock.last_updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                    needs_update = True
            
            # Categorize the stock
            if needs_update:
                result.to_update.append(updated_stock)
            else:
                # Truly unchanged - all data matches both sources and Yahoo Finance
                result.unchanged.append(symbol)
        
        # Log Yahoo API failures and mark for removal
        if yahoo_failures:
            logger.warning(f"Yahoo Finance API failures for {len(yahoo_failures)} symbols - these will be removed from database: {yahoo_failures[:10]}{'...' if len(yahoo_failures) > 10 else ''}")
            result.to_remove_due_to_errors.extend(yahoo_failures)
            result.validation_failures.extend(yahoo_failures)
    
    logger.info(f"Comprehensive synchronization analysis complete: {result.get_stats()}")
    logger.info("All existing stocks have been verified against Yahoo Finance API for data accuracy")
    return result