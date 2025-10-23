"""
Data Layer Package for Stock Information Website

This package provides a comprehensive data access layer for stock-related operations,
including database connection management, models, and repositories with full CRUD capabilities.
"""

from .models.stock import Stock
from .repositories.stock_repository import StockRepository
from .database.connection_manager import DatabaseConnectionManager
from .exceptions import (
    DataLayerError,
    DatabaseConnectionError,
    StockNotFoundError,
    DuplicateStockError,
    ValidationError
)

__version__ = "1.0.0"
__all__ = [
    "Stock",
    "StockRepository", 
    "DatabaseConnectionManager",
    "DataLayerError",
    "DatabaseConnectionError",
    "StockNotFoundError",
    "DuplicateStockError",
    "ValidationError"
]