"""
Repositories package initialization.
"""

from .base_repository import BaseRepository
from .stocks_repository import StocksRepository

__all__ = ["BaseRepository", "StocksRepository"]