"""
Constants for ticker summary table synchronization.
"""

# Batch size for both Yahoo Finance API lookups and database persistence operations
# Using the same batch size ensures immediate persistence after each lookup batch
BATCH_SIZE = 50

# Max concurrent workers for Yahoo Finance API requests
MAX_WORKERS = 8
 
