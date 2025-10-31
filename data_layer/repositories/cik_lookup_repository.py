"""
CIK lookup repository for database operations.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple, Any
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
    Supports indexing by CIK (primary key) and company name.
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
    
    def create(self, entity: CikLookup) -> CikLookup:
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
        # Validate the CIK lookup
        cik_lookup.validate()
        
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
        cik_lookup.validate()
        
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
        query = """
        SELECT cik, company_name, created_at, last_updated_at
        FROM cik_lookup
        ORDER BY cik
        """
        
        params: List[int] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        if offset is not None:
            query += " OFFSET %s"
            params.append(offset)
        
        query += ";"
        
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
    
    def bulk_create(self, entities: List[CikLookup]) -> Tuple[int, List[int]]:
        """
        Create multiple CIK lookup entries in a single transaction.
        
        Args:
            entities: List of CikLookup entities to create
        
        Returns:
            Tuple of (number of successful inserts, list of failed CIKs)
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0, []
        
        # Validate all entities first
        for entity in entities:
            entity.validate()
        
        insert_query = """
        INSERT INTO cik_lookup (cik, company_name, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cik) DO NOTHING;
        """
        
        current_time = datetime.now()
        successful = 0
        failed_ciks: List[int] = []
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                for entity in entities:
                    try:
                        cursor.execute(insert_query, (
                            entity.cik,
                            entity.company_name,
                            current_time,
                            current_time
                        ))
                        if cursor.rowcount > 0:
                            successful += 1
                            entity.created_at = current_time
                            entity.last_updated_at = current_time
                        else:
                            failed_ciks.append(entity.cik)
                    except Exception as e:
                        self.logger.warning(f"Failed to insert CIK {entity.cik}: {e}")
                        failed_ciks.append(entity.cik)
                
                self.logger.info(f"Bulk created {successful} CIK lookups, {len(failed_ciks)} failed")
                return successful, failed_ciks
                
        except Exception as e:
            raise DatabaseQueryError("bulk create CIK lookups", str(e))
    
    def upsert(self, entity: CikLookup) -> CikLookup:
        """
        Insert or update a CIK lookup entry (upsert).
        If the CIK exists, updates the company name and last_updated_at.
        If the CIK doesn't exist, creates a new entry.
        
        Args:
            entity: CikLookup entity to upsert
        
        Returns:
            Upserted CikLookup with timestamps
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        entity.validate()
        
        upsert_query = """
        INSERT INTO cik_lookup (cik, company_name, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cik) 
        DO UPDATE SET 
            company_name = EXCLUDED.company_name,
            last_updated_at = EXCLUDED.last_updated_at
        RETURNING created_at, last_updated_at;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                cursor.execute(upsert_query, (
                    entity.cik,
                    entity.company_name,
                    current_time,
                    current_time
                ))
                
                result = cursor.fetchone()
                
                entity.created_at = result[0]
                entity.last_updated_at = result[1]
                
                self.logger.info(f"Upserted CIK lookup: {entity.cik}")
                return entity
                
        except Exception as e:
            raise DatabaseQueryError("upsert CIK lookup", str(e))
    
    def bulk_upsert(self, entities: List[CikLookup]) -> int:
        """
        Insert or update multiple CIK lookup entries in a single transaction.
        
        Args:
            entities: List of CikLookup entities to upsert
        
        Returns:
            Number of rows affected
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        # Validate all entities first
        for entity in entities:
            entity.validate()
        
        upsert_query = """
        INSERT INTO cik_lookup (cik, company_name, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cik) 
        DO UPDATE SET 
            company_name = EXCLUDED.company_name,
            last_updated_at = EXCLUDED.last_updated_at;
        """
        
        current_time = datetime.now()
        total_affected = 0
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                for entity in entities:
                    cursor.execute(upsert_query, (
                        entity.cik,
                        entity.company_name,
                        current_time,
                        current_time
                    ))
                    total_affected += cursor.rowcount
                    entity.created_at = current_time
                    entity.last_updated_at = current_time
                
                self.logger.info(f"Bulk upserted {total_affected} CIK lookups")
                return total_affected
                
        except Exception as e:
            raise DatabaseQueryError("bulk upsert CIK lookups", str(e))
    
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
