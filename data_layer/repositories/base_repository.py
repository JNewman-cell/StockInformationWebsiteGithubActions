"""
Abstract base repository class.
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional

from ..database.connection_manager import DatabaseConnectionManager


T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base class for repositories providing common database operations.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the repository with a database manager.
        
        Args:
            db_manager: Database connection manager instance
        """
        self.db_manager = db_manager
    
    @abstractmethod
    def create(self, entity: T) -> T:
        """
        Create a new entity in the database.
        
        Args:
            entity: Entity to create
        
        Returns:
            Created entity with updated fields (e.g., ID, timestamps)
        """
        pass
    
    
    @abstractmethod
    def update(self, entity: T) -> T:
        """
        Update an existing entity in the database.
        
        Args:
            entity: Entity to update
        
        Returns:
            Updated entity
        """
        pass
    
    @abstractmethod
    def delete(self, entity_id: int) -> bool:
        """
        Delete an entity by its ID.
        
        Args:
            entity_id: The ID of the entity to delete
        
        Returns:
            True if deletion was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[T]:
        """
        Retrieve all entities with optional pagination.
        
        Args:
            limit: Maximum number of entities to return
            offset: Number of entities to skip
        
        Returns:
            List of entities
        """
        pass
    
    @abstractmethod
    def count(self) -> int:
        """
        Count the total number of entities.
        
        Returns:
            Total count of entities
        """
        pass
    