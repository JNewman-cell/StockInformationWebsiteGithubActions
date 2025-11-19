"""
Entity class for synchronization results.

This module contains the SynchronizationResult entity class used to organize
and track the results of ticker overview synchronization operations.
"""

import os
import sys
from typing import Dict, List

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models.ticker_overview import TickerOverview


class SynchronizationResult:
    """Container for synchronization operation results."""
    
    def __init__(self):
        self.to_add: List[TickerOverview] = []  # ticker overviews to add
        self.to_update: List[TickerOverview] = []  # ticker overviews to update
        self.unchanged: List[str] = []  # ticker symbols that are unchanged
        self.failed_ticker_lookups: List[str] = []  # ticker symbols that failed Yahoo Finance API lookup
        self.to_remove_due_to_errors: List[str] = []  # ticker symbols to remove due to persistent API errors
        
    def get_stats(self) -> Dict[str, int]:
        """Get summary statistics."""
        return {
            'to_add': len(self.to_add),
            'to_update': len(self.to_update),
            'unchanged': len(self.unchanged),
            'failed_ticker_lookups': len(self.failed_ticker_lookups),
            'to_remove_due_to_errors': len(self.to_remove_due_to_errors)
        }
