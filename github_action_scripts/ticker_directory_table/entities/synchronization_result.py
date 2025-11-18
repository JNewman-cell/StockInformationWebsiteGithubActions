"""
Entity class for ticker directory synchronization results.

This module contains the SynchronizationResult entity class used to organize
and track the results of ticker directory synchronization operations.
"""

import os
import sys
from typing import Dict, List

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import TickerDirectory


class SynchronizationResult:
    """Container for ticker directory synchronization operation results."""
    
    def __init__(self):
        self.to_add: List['TickerDirectory'] = []  # Ticker directory entries to add
        self.to_update_to_inactive: List[str] = []  # Tickers to update to INACTIVE status
        self.unchanged: List[str] = []  # Tickers that are unchanged
        self.failed_ticker_lookups: List[str] = []  # Tickers that failed CIK lookup
        
    def get_stats(self) -> Dict[str, int]:
        """Get summary statistics."""
        return {
            'to_add': len(self.to_add),
            'to_update_to_inactive': len(self.to_update_to_inactive),
            'unchanged': len(self.unchanged),
            'failed_ticker_lookups': len(self.failed_ticker_lookups),
        }
