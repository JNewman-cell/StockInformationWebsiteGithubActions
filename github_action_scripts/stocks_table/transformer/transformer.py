"""
Stock synchronization transformer functions.

This module contains transformation and comparison logic for stock synchronization.
"""

import logging
import os
import sys
from typing import Dict, List, Set, Tuple

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer import Stock
from entities.synchronization_result import SynchronizationResult

logger = logging.getLogger(__name__)


def analyze_database_vs_source_symbols_for_synchronization_operations(
    database_stocks: Dict[str, Stock], 
    source_symbols: Set[Tuple[str, str]]
) -> SynchronizationResult:
    """
    Compare database stocks with source symbols to determine sync operations.
    
    This function implements the deterministic synchronization logic:
    - Source symbols not in database -> add to database (after validation)
    - Database symbols not in source -> delete from database
    - Symbols in both -> check for updates needed
    
    Args:
        database_stocks: Dictionary mapping symbol to Stock object from database
        source_symbols: Set of (symbol, exchange) tuples from data sources
        
    Returns:
        SynchronizationResult object containing all operations to perform
    """
    result = SynchronizationResult()
    
    # Create sets for efficient comparison
    db_symbols = set(database_stocks.keys())
    source_symbol_names = {symbol for symbol, exchange in source_symbols}
    
    # Find symbols to add (in source but not in database)
    symbols_to_add = source_symbol_names - db_symbols
    for symbol in symbols_to_add:
        # Find the exchange for this symbol from source_symbols
        exchange = next((exch for sym, exch in source_symbols if sym == symbol), None)
        result.to_add.append((symbol, exchange))
    
    # Find symbols to delete (in database but not in source)
    symbols_to_delete = db_symbols - source_symbol_names
    result.to_delete.extend(symbols_to_delete)
    
    # Find symbols that exist in both (might need updates)
    common_symbols = db_symbols & source_symbol_names
    for symbol in common_symbols:
        db_stock = database_stocks[symbol]
        # Find the source exchange for comparison
        source_exchange = next((exch for sym, exch in source_symbols if sym == symbol), None)
        
        # Check if exchange information differs (needs update)
        if db_stock.exchange != source_exchange:
            # Create updated stock object
            updated_stock = Stock(
                id=db_stock.id,
                symbol=db_stock.symbol,
                company=db_stock.company,  # Keep existing company name for now
                exchange=source_exchange,  # Update to source exchange
                created_at=db_stock.created_at,
                last_updated_at=db_stock.last_updated_at
            )
            result.to_update.append(updated_stock)
        else:
            result.unchanged.append(symbol)
    
    logger.info(f"Synchronization analysis complete: {result.get_stats()}")
    return result