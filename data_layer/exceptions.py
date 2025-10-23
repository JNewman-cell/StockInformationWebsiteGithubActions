"""
Custom exceptions for the data layer.
"""


class DataLayerError(Exception):
    """Base exception for all data layer errors."""
    pass


class DatabaseConnectionError(DataLayerError):
    """Raised when database connection fails."""
    pass


class StockNotFoundError(DataLayerError):
    """Raised when a stock is not found in the database."""
    
    def __init__(self, identifier: str, identifier_type: str = "symbol"):
        self.identifier = identifier
        self.identifier_type = identifier_type
        super().__init__(f"Stock not found with {identifier_type}: {identifier}")


class DuplicateStockError(DataLayerError):
    """Raised when attempting to create a stock that already exists."""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        super().__init__(f"Stock with symbol '{symbol}' already exists")


class ValidationError(DataLayerError):
    """Raised when data validation fails."""
    
    def __init__(self, field: str, value, message: str):
        self.field = field
        self.value = value
        super().__init__(f"Validation error for field '{field}': {message}")


class DatabaseQueryError(DataLayerError):
    """Raised when a database query fails."""
    
    def __init__(self, operation: str, error: str):
        self.operation = operation
        super().__init__(f"Database query failed for operation '{operation}': {error}")