"""
Abstract base repository class.
"""

import logging
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional

from ..database.connection_manager import DatabaseConnectionManager


T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base class for repositories providing common database operations.
    Organized by: CREATE, READ, UPDATE, DELETE operations.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the repository with a database manager.
        
        Args:
            db_manager: Database connection manager instance
        """
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self.table_name = ""  # To be set by subclasses
    
    # ============================================================================
    # CREATE OPERATIONS
    # ============================================================================
    
    @abstractmethod
    def insert(self, entity: T) -> T:
        """
        Create a new entity in the database.
        
        Args:
            entity: Entity to create
        
        Returns:
            Created entity with updated fields (e.g., ID, timestamps)
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    @abstractmethod
    def bulk_insert(self, entities: List[T]) -> int:
        """
        Insert multiple entities in a single transaction.
        
        Args:
            entities: List of entities to insert
        
        Returns:
            Number of rows successfully inserted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    # ============================================================================
    # READ OPERATIONS
    # ============================================================================
    
    @abstractmethod
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[T]:
        """
        Retrieve all entities with optional pagination.
        
        Args:
            limit: Maximum number of entities to return
            offset: Number of entities to skip
        
        Returns:
            List of entities
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    @abstractmethod
    def count(self) -> int:
        """
        Count the total number of entities.
        
        Returns:
            Total count of entities
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    # ============================================================================
    # UPDATE OPERATIONS
    # ============================================================================
    
    @abstractmethod
    def update(self, entity: T) -> T:
        """
        Update an existing entity in the database.
        
        Args:
            entity: Entity to update
        
        Returns:
            Updated entity
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    @abstractmethod
    def bulk_update(self, entities: List[T]) -> int:
        """
        Update multiple existing entities in a single transaction.
        
        Args:
            entities: List of entities to update
        
        Returns:
            Number of rows successfully updated
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    # ============================================================================
    # DELETE OPERATIONS
    # ============================================================================
    
    @abstractmethod
    def delete(self, entity_id: int) -> bool:
        """
        Delete an entity by its ID.
        
        Args:
            entity_id: The ID of the entity to delete
        
        Returns:
            True if deletion was successful, False otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    
    @abstractmethod
    def bulk_delete(self, entity_ids: List[int]) -> int:
        """
        Delete multiple entities in a single transaction.
        
        Args:
            entity_ids: List of entity IDs to delete
        
        Returns:
            Number of rows successfully deleted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        pass
    