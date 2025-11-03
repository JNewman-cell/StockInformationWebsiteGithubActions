"""
Entity class for CIK lookup synchronization results.

This module contains the SynchronizationResult entity class used to organize
and track the results of CIK lookup synchronization operations.
"""

import os
import sys
from typing import Dict, List

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import CikLookup


class SynchronizationResult:
    """Container for CIK lookup synchronization operation results."""
    
    def __init__(self):
        self.to_add: List[CikLookup] = []  # CIK entries to add
        self.to_delete: List[int] = []  # CIKs to delete
        self.to_update: List[CikLookup] = []  # CIK entries to update
        self.unchanged: List[int] = []  # CIKs that are unchanged
        self.failed_ticker_lookups: List[str] = []  # tickers that failed CIK lookup
        
    def get_stats(self) -> Dict[str, int]:
        """Get summary statistics."""
        return {
            'to_add': len(self.to_add),
            'to_delete': len(self.to_delete),
            'to_update': len(self.to_update),
            'unchanged': len(self.unchanged),
            'failed_ticker_lookups': len(self.failed_ticker_lookups),
        }
