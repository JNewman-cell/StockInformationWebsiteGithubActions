"""
CIK lookup repository for database operations.
"""

import logging
from datetime import datetime
from typing import List, Optional, Any
import psycopg

from .base_repository import BaseRepository
from ..models.cik_lookup import CikLookup
from ..database.connection_manager import DatabaseConnectionManager
from ..exceptions import DatabaseQueryError


class CikLookupNotFoundError(Exception):
    """Exception raised when a CIK lookup is not found."""
    
    def __init__(self, identifier: str, value: Any):
        self.identifier = identifier
        self.value = value
        super().__init__(f"CIK lookup not found by {identifier}: {value}")


class DuplicateCikError(Exception):
    """Exception raised when attempting to create a duplicate CIK."""
    
    def __init__(self, cik: int):
        self.cik = cik
        super().__init__(f"CIK already exists: {cik}")


class CikLookupRepository(BaseRepository[CikLookup]):
    """
    Repository for CIK lookup entities with full CRUD operations.
    Organized by: CREATE, READ, UPDATE, DELETE operations.
    Supports searching by CIK (primary key) and company name.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the CIK lookup repository.
        
        Args:
            db_manager: Database connection manager instance
        """
        super().__init__(db_manager)
        self.logger = logging.getLogger(__name__)
        self.table_name = "cik_lookup"
    
    # ============================================================================
    # CREATE OPERATIONS
    # ============================================================================
    
    def insert(self, entity: CikLookup) -> CikLookup:
        """
        Create a new CIK lookup entry in the database.
        
        Args:
            entity: CikLookup entity to create
        
        Returns:
            Created CIK lookup with timestamps
        
        Raises:
            DuplicateCikError: If CIK already exists
            DatabaseQueryError: If database operation fails
        """
        cik_lookup = entity
        
        # Check if CIK already exists
        if self.get_by_cik(cik_lookup.cik) is not None:
            raise DuplicateCikError(cik_lookup.cik)
        
        insert_query = """
        INSERT INTO cik_lookup (cik, company_name, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s)
        RETURNING created_at, last_updated_at;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                cursor.execute(insert_query, (
                    cik_lookup.cik,
                    cik_lookup.company_name,
                    current_time,
                    current_time
                ))
                
                result = cursor.fetchone()
                
                # Update the object with database-generated values
                cik_lookup.created_at = result[0]
                cik_lookup.last_updated_at = result[1]
                
                self.logger.info(f"Created CIK lookup: {cik_lookup.cik} - {cik_lookup.company_name}")
                return cik_lookup
                
        except psycopg.errors.UniqueViolation:
            raise DuplicateCikError(cik_lookup.cik)
        except Exception as e:
            raise DatabaseQueryError("create CIK lookup", str(e))
    
    def bulk_insert(self, entities: List[CikLookup]) -> int:
        """
        Insert multiple CIK lookup entries in a single transaction.
        Skips entries that already exist (uses ON CONFLICT DO NOTHING).
        
        Args:
            entities: List of CikLookup entities to insert
        
        Returns:
            Number of rows successfully inserted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        insert_query = """
        INSERT INTO cik_lookup (cik, company_name, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cik) DO NOTHING;
        """
        
        current_time = datetime.now()
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                # Prepare data for batch insert
                data = [
                    (entity.cik, entity.company_name, current_time, current_time)
                    for entity in entities
                ]
                
                # Use executemany for efficient batch insert
                cursor.executemany(insert_query, data)
                total_inserted = cursor.rowcount
                
                # Update entities with timestamps
                for entity in entities:
                    entity.created_at = current_time
                    entity.last_updated_at = current_time
                
                self.logger.info(f"Bulk inserted {total_inserted} new CIK lookups")
                return total_inserted
                
        except Exception as e:
            raise DatabaseQueryError("bulk insert CIK lookups", str(e))
    
    # ============================================================================
    # READ OPERATIONS
    # ============================================================================
    
    def get_by_cik(self, cik: int) -> Optional[CikLookup]:
        """
        Retrieve a CIK lookup entry by its CIK (primary key).
        
        Args:
            cik: The CIK to retrieve
        
        Returns:
            CikLookup if found, None otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        select_query = """
        SELECT cik, company_name, created_at, last_updated_at
        FROM cik_lookup
        WHERE cik = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, (cik,))
                result = cursor.fetchone()
                
                if result:
                    return CikLookup(
                        cik=result[0],
                        company_name=result[1],
                        created_at=result[2],
                        last_updated_at=result[3]
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving CIK lookup by CIK {cik}: {e}")
            raise DatabaseQueryError("get CIK lookup by CIK", str(e))
    
    def get_by_company_name(self, company_name: str, exact_match: bool = True) -> Optional[CikLookup]:
        """
        Retrieve a CIK lookup entry by company name.
        
        Args:
            company_name: The company name to search for
            exact_match: If True, performs exact match (case-insensitive).
                        If False, performs partial match using LIKE.
        
        Returns:
            CikLookup if found (returns first match if multiple), None otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if exact_match:
            select_query = """
            SELECT cik, company_name, created_at, last_updated_at
            FROM cik_lookup
            WHERE LOWER(company_name) = LOWER(%s)
            LIMIT 1;
            """
            params = (company_name.strip(),)
        else:
            select_query = """
            SELECT cik, company_name, created_at, last_updated_at
            FROM cik_lookup
            WHERE LOWER(company_name) LIKE LOWER(%s)
            LIMIT 1;
            """
            params = (f"%{company_name.strip()}%",)
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, params)
                result = cursor.fetchone()
                
                if result:
                    return CikLookup(
                        cik=result[0],
                        company_name=result[1],
                        created_at=result[2],
                        last_updated_at=result[3]
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving CIK lookup by company name {company_name}: {e}")
            raise DatabaseQueryError("get CIK lookup by company name", str(e))
    
    def search_by_company_name(self, company_name: str, limit: int = 10) -> List[CikLookup]:
        """
        Search for CIK lookup entries by company name (partial match).
        
        Args:
            company_name: The company name to search for
            limit: Maximum number of results to return
        
        Returns:
            List of matching CikLookup entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        select_query = """
        SELECT cik, company_name, created_at, last_updated_at
        FROM cik_lookup
        WHERE LOWER(company_name) LIKE LOWER(%s)
        ORDER BY company_name
        LIMIT %s;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, (f"%{company_name.strip()}%", limit))
                results = cursor.fetchall()
                
                return [
                    CikLookup(
                        cik=row[0],
                        company_name=row[1],
                        created_at=row[2],
                        last_updated_at=row[3]
                    )
                    for row in results
                ]
                
        except Exception as e:
            self.logger.error(f"Error searching CIK lookup by company name {company_name}: {e}")
            raise DatabaseQueryError("search CIK lookup by company name", str(e))
    
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[CikLookup]:
        """
        Retrieve all CIK lookup entries with optional pagination.
        
        Args:
            limit: Maximum number of entries to return
            offset: Number of entries to skip
        
        Returns:
            List of CikLookup entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        # Use parameterized query parts for safe SQL construction
        query_parts = ["""
        SELECT cik, company_name, created_at, last_updated_at
        FROM cik_lookup
        ORDER BY cik"""]
        
        params: List[int] = []
        
        if limit is not None:
            query_parts.append(" LIMIT %s")
            params.append(limit)
        if offset is not None:
            query_parts.append(" OFFSET %s")
            params.append(offset)
        
        query = "".join(query_parts) + ";"
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, tuple(params) if params else None)
                results = cursor.fetchall()
                
                return [
                    CikLookup(
                        cik=row[0],
                        company_name=row[1],
                        created_at=row[2],
                        last_updated_at=row[3]
                    )
                    for row in results
                ]
                
        except Exception as e:
            self.logger.error(f"Error retrieving all CIK lookups: {e}")
            raise DatabaseQueryError("get all CIK lookups", str(e))
    
    def count(self) -> int:
        """
        Count the total number of CIK lookup entries.
        
        Returns:
            Total count of entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        count_query = "SELECT COUNT(*) FROM cik_lookup;"
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            self.logger.error(f"Error counting CIK lookups: {e}")
            raise DatabaseQueryError("count CIK lookups", str(e))
    
    def exists(self, cik: int) -> bool:
        """
        Check if a CIK exists in the database.
        
        Args:
            cik: The CIK to check
        
        Returns:
            True if the CIK exists, False otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        query = "SELECT 1 FROM cik_lookup WHERE cik = %s LIMIT 1;"
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, (cik,))
                return cursor.fetchone() is not None
                
        except Exception as e:
            self.logger.error(f"Error checking if CIK {cik} exists: {e}")
            raise DatabaseQueryError("check CIK exists", str(e))
    
    # ============================================================================
    # UPDATE OPERATIONS
    # ============================================================================
    
    def update(self, entity: CikLookup) -> CikLookup:
        """
        Update an existing CIK lookup entry in the database.
        
        Args:
            entity: CikLookup entity to update (must have CIK as PK)
        
        Returns:
            Updated CikLookup
        
        Raises:
            CikLookupNotFoundError: If CIK doesn't exist
            ValidationError: If CIK is missing
            DatabaseQueryError: If database operation fails
        """
        cik_lookup = entity
        
        # Check if the CIK exists
        existing = self.get_by_cik(cik_lookup.cik)
        if existing is None:
            raise CikLookupNotFoundError("cik", cik_lookup.cik)
        
        update_query = """
        UPDATE cik_lookup
        SET company_name = %s, last_updated_at = %s
        WHERE cik = %s
        RETURNING created_at, last_updated_at;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                cursor.execute(update_query, (
                    cik_lookup.company_name,
                    current_time,
                    cik_lookup.cik
                ))
                
                result = cursor.fetchone()
                
                # Update the object with database values
                cik_lookup.created_at = result[0]
                cik_lookup.last_updated_at = result[1]
                
                self.logger.info(f"Updated CIK lookup: {cik_lookup.cik}")
                return cik_lookup
                
        except Exception as e:
            raise DatabaseQueryError("update CIK lookup", str(e))
    
    def update_company_name(self, cik: int, company_name: str) -> CikLookup:
        """
        Update the company name for a specific CIK.
        
        Args:
            cik: The CIK to update
            company_name: The new company name
        
        Returns:
            Updated CikLookup
        
        Raises:
            CikLookupNotFoundError: If CIK doesn't exist
            DatabaseQueryError: If database operation fails
        """
        # Get existing entry
        existing = self.get_by_cik(cik)
        if existing is None:
            raise CikLookupNotFoundError("cik", cik)
        
        # Update the company name
        existing.company_name = company_name
        return self.update(existing)
    
    def bulk_update(self, entities: List[CikLookup]) -> int:
        """
        Update multiple existing CIK lookup entries in a single transaction.
        Only updates entries that already exist in the database.
        
        Args:
            entities: List of CikLookup entities to update
        
        Returns:
            Number of rows successfully updated
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        update_query = """
        UPDATE cik_lookup
        SET company_name = %s, last_updated_at = %s
        WHERE cik = %s;
        """
        
        current_time = datetime.now()
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                # Prepare data for batch update
                data = [
                    (entity.company_name, current_time, entity.cik)
                    for entity in entities
                ]
                
                # Use executemany for efficient batch update
                cursor.executemany(update_query, data)
                total_updated = cursor.rowcount
                
                # Update entities with new timestamp
                for entity in entities:
                    entity.last_updated_at = current_time
                
                self.logger.info(f"Bulk updated {total_updated} CIK lookups")
                return total_updated
                
        except Exception as e:
            raise DatabaseQueryError("bulk update CIK lookups", str(e))
    
    # ============================================================================
    # DELETE OPERATIONS
    # ============================================================================
    
    def delete(self, entity_id: int) -> bool:
        """
        Delete a CIK lookup entry by its CIK.
        
        Args:
            entity_id: The CIK of the entry to delete
        
        Returns:
            True if deletion was successful, False if not found
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        cik = entity_id
        delete_query = """
        DELETE FROM cik_lookup
        WHERE cik = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(delete_query, (cik,))
                deleted = cursor.rowcount > 0
                
                if deleted:
                    self.logger.info(f"Deleted CIK lookup: {cik}")
                
                return deleted
                
        except Exception as e:
            raise DatabaseQueryError("delete CIK lookup", str(e))
    
    def bulk_delete(self, entity_ids: List[int]) -> int:
        """
        Delete multiple CIK lookup entries in a single transaction.
        
        Args:
            entity_ids: List of CIK values to delete
        
        Returns:
            Number of rows successfully deleted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entity_ids:
            return 0
        
        delete_query = """
        DELETE FROM cik_lookup
        WHERE cik = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                # Prepare data for batch delete - executemany expects tuples
                data = [(cik,) for cik in entity_ids]
                
                # Use executemany for efficient batch delete
                cursor.executemany(delete_query, data)
                total_deleted = cursor.rowcount
                
                self.logger.info(f"Bulk deleted {total_deleted} CIK lookups")
                return total_deleted
                
        except Exception as e:
            raise DatabaseQueryError("bulk delete CIK lookups", str(e))
