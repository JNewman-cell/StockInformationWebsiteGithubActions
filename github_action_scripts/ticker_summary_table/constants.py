"""
Constants for ticker summary table synchronization.
"""

# Batch size for both Yahoo Finance API lookups and database persistence operations
# Using the same batch size ensures immediate persistence after each lookup batch
BATCH_SIZE = 50

# Max concurrent workers for Yahoo Finance API requests
MAX_WORKERS = 8

# Maximum number of retry attempts for crumb failures
MAX_CRUMB_RETRIES = 3

# Delay in seconds between retry attempts for crumb failures
CRUMB_RETRY_DELAY = 2.0
