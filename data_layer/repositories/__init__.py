"""
Repositories package initialization.
"""

from .base_repository import BaseRepository
from .stock_repository import StockRepository

__all__ = ["BaseRepository", "StockRepository"]