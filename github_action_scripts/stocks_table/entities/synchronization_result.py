"""
Entity class for synchronization results.

This module contains the SynchronizationResult entity class used to organize
and track the results of stock synchronization operations.
"""

import os
import sys
from typing import Dict, List, Tuple

# Add data layer to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer import Stock


class SynchronizationResult:
    """Container for synchronization operation results."""
    
    def __init__(self):
        self.to_add: List[Tuple[str, str]] = []  # (symbol, exchange) pairs to add
        self.to_delete: List[str] = []  # symbols to delete
        self.to_update: List[Stock] = []  # stocks to update
        self.unchanged: List[str] = []  # symbols that are unchanged
        self.validation_failures: List[str] = []  # symbols that failed validation
        self.to_remove_due_to_errors: List[str] = []  # symbols to remove due to persistent API errors
        
    def get_stats(self) -> Dict[str, int]:
        """Get summary statistics."""
        return {
            'to_add': len(self.to_add),
            'to_delete': len(self.to_delete),
            'to_update': len(self.to_update),
            'unchanged': len(self.unchanged),
            'validation_failures': len(self.validation_failures),
            'to_remove_due_to_errors': len(self.to_remove_due_to_errors)
        }