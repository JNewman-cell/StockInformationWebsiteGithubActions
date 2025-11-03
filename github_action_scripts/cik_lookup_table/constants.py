"""
Constants for CIK lookup table synchronization.
"""

# Batch size for both CIK lookups from sec-company-lookup API and database persistence operations
# Using the same batch size ensures immediate persistence after each lookup batch
BATCH_SIZE = 100
