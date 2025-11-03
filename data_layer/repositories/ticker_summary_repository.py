"""
Ticker summary repository for database operations.
"""

import logging
import psycopg
from typing import List, Optional, Any

from .base_repository import BaseRepository
from ..models.ticker_summary import TickerSummary
from ..database.connection_manager import DatabaseConnectionManager
from ..exceptions import DatabaseQueryError


class TickerSummaryNotFoundError(Exception):
    """Exception raised when a ticker summary is not found."""
    
    def __init__(self, identifier: str, value: Any):
        self.identifier = identifier
        self.value = value
        super().__init__(f"Ticker summary not found by {identifier}: {value}")


class DuplicateTickerError(Exception):
    """Exception raised when attempting to create a duplicate ticker."""
    
    def __init__(self, ticker: str):
        self.ticker = ticker
        super().__init__(f"Ticker already exists: {ticker}")


class TickerSummaryRepository(BaseRepository[TickerSummary]):
    """
    Repository for ticker summary entities with full CRUD operations.
    Organized by: CREATE, READ, UPDATE, DELETE operations.
    Supports searching by ticker (primary key) and filtering by various metrics.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the ticker summary repository.
        
        Args:
            db_manager: Database connection manager instance
        """
        super().__init__(db_manager)
        self.logger = logging.getLogger(__name__)
        self.table_name = "ticker_summary"
    
    # ============================================================================
    # CREATE OPERATIONS
    # ============================================================================
    
    def insert(self, entity: TickerSummary) -> TickerSummary:
        """
        Create a new ticker summary entry in the database.
        
        Args:
            entity: TickerSummary entity to create
        
        Returns:
            Created ticker summary
        
        Raises:
            DuplicateTickerError: If ticker already exists
            DatabaseQueryError: If database operation fails
        """
        ticker_summary = entity
        
        # Check if ticker already exists
        if self.get_by_ticker(ticker_summary.ticker) is not None:
            raise DuplicateTickerError(ticker_summary.ticker)
        
        insert_query = """
        INSERT INTO ticker_summary (
            ticker, cik, market_cap, previous_close, pe_ratio, 
            forward_pe_ratio, dividend_yield, payout_ratio, 
            fifty_day_average, two_hundred_day_average
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        (
                            ticker_summary.ticker,
                            ticker_summary.cik,
                            ticker_summary.market_cap,
                            ticker_summary.previous_close,
                            ticker_summary.pe_ratio,
                            ticker_summary.forward_pe_ratio,
                            ticker_summary.dividend_yield,
                            ticker_summary.payout_ratio,
                            ticker_summary.fifty_day_average,
                            ticker_summary.two_hundred_day_average
                        )
                    )
                    conn.commit()
                    self.logger.info(f"Successfully inserted ticker summary: {ticker_summary.ticker}")
                    return ticker_summary
                
        except psycopg.errors.UniqueViolation:
            raise DuplicateTickerError(ticker_summary.ticker)
        except Exception as e:
            raise DatabaseQueryError("insert ticker summary", str(e))
    
    def bulk_insert(self, entities: List[TickerSummary]) -> int:
        """
        Insert multiple ticker summary entries in a single transaction.
        Skips entries that already exist (uses ON CONFLICT DO NOTHING).
        
        Args:
            entities: List of TickerSummary entities to insert
        
        Returns:
            Number of rows successfully inserted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        insert_query = """
        INSERT INTO ticker_summary (
            ticker, cik, market_cap, previous_close, pe_ratio, 
            forward_pe_ratio, dividend_yield, payout_ratio, 
            fifty_day_average, two_hundred_day_average
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO NOTHING;
        """
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    data = [
                        (
                            ts.ticker,
                            ts.cik,
                            ts.market_cap,
                            ts.previous_close,
                            ts.pe_ratio,
                            ts.forward_pe_ratio,
                            ts.dividend_yield,
                            ts.payout_ratio,
                            ts.fifty_day_average,
                            ts.two_hundred_day_average
                        )
                        for ts in entities
                    ]
                    cursor.executemany(insert_query, data)
                    rows_inserted = cursor.rowcount
                    conn.commit()
                    self.logger.info(f"Successfully bulk inserted {rows_inserted} ticker summaries")
                    return rows_inserted
                
        except Exception as e:
            raise DatabaseQueryError("bulk insert ticker summaries", str(e))
    
    # ============================================================================
    # READ OPERATIONS
    # ============================================================================
    
    def get_by_ticker(self, ticker: str) -> Optional[TickerSummary]:
        """
        Retrieve a ticker summary entry by its ticker symbol (primary key).
        
        Args:
            ticker: The ticker symbol to retrieve
        
        Returns:
            TickerSummary if found, None otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        select_query = """
        SELECT ticker, cik, market_cap, previous_close, pe_ratio, 
               forward_pe_ratio, dividend_yield, payout_ratio, 
               fifty_day_average, two_hundred_day_average
        FROM ticker_summary
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (ticker.upper(),))
                    row = cursor.fetchone()
                    
                    if row is None:
                        return None
                    
                    return self._row_to_entity(row)
                
        except Exception as e:
            raise DatabaseQueryError("get ticker summary by ticker", str(e))
    
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[TickerSummary]:
        """
        Retrieve all ticker summary entries with optional pagination.
        
        Args:
            limit: Maximum number of entries to return
            offset: Number of entries to skip
        
        Returns:
            List of TickerSummary entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        # Use parameterized query parts for safe SQL construction
        query_parts = ["""
        SELECT ticker, cik, market_cap, previous_close, pe_ratio, 
               forward_pe_ratio, dividend_yield, payout_ratio, 
               fifty_day_average, two_hundred_day_average
        FROM ticker_summary
        ORDER BY ticker"""]
        
        params: List[int] = []
        
        if limit is not None:
            query_parts.append(" LIMIT %s")
            params.append(limit)
        if offset is not None:
            query_parts.append(" OFFSET %s")
            params.append(offset)
        
        query = "".join(query_parts) + ";"
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)  # type: ignore[arg-type]
                    rows = cursor.fetchall()
                    
                    return [self._row_to_entity(row) for row in rows]
                
        except Exception as e:
            raise DatabaseQueryError("get all ticker summaries", str(e))
    
    def count(self) -> int:
        """
        Count the total number of ticker summary entries.
        
        Returns:
            Total count of entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        count_query = "SELECT COUNT(*) FROM ticker_summary;"
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(count_query)
                    result = cursor.fetchone()
                    return result[0] if result else 0
                
        except Exception as e:
            raise DatabaseQueryError("count ticker summaries", str(e))
    
    def exists(self, ticker: str) -> bool:
        """
        Check if a ticker exists in the database.
        
        Args:
            ticker: The ticker symbol to check
        
        Returns:
            True if the ticker exists, False otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        query = "SELECT 1 FROM ticker_summary WHERE ticker = %s LIMIT 1;"
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ticker.upper(),))
                    return cursor.fetchone() is not None
                
        except Exception as e:
            raise DatabaseQueryError("check ticker existence", str(e))
    
    # ============================================================================
    # UPDATE OPERATIONS
    # ============================================================================
    
    def update(self, entity: TickerSummary) -> TickerSummary:
        """
        Update an existing ticker summary entry in the database.
        
        Args:
            entity: TickerSummary entity to update (must have ticker as PK)
        
        Returns:
            Updated TickerSummary
        
        Raises:
            TickerSummaryNotFoundError: If ticker doesn't exist
            DatabaseQueryError: If database operation fails
        """
        ticker_summary = entity
        
        # Check if the ticker exists
        existing = self.get_by_ticker(ticker_summary.ticker)
        if existing is None:
            raise TickerSummaryNotFoundError("ticker", ticker_summary.ticker)
        
        update_query = """
        UPDATE ticker_summary
        SET cik = %s, market_cap = %s, previous_close = %s, pe_ratio = %s,
            forward_pe_ratio = %s, dividend_yield = %s, payout_ratio = %s,
            fifty_day_average = %s, two_hundred_day_average = %s
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            ticker_summary.cik,
                            ticker_summary.market_cap,
                            ticker_summary.previous_close,
                            ticker_summary.pe_ratio,
                            ticker_summary.forward_pe_ratio,
                            ticker_summary.dividend_yield,
                            ticker_summary.payout_ratio,
                            ticker_summary.fifty_day_average,
                            ticker_summary.two_hundred_day_average,
                            ticker_summary.ticker
                        )
                    )
                    conn.commit()
                    self.logger.info(f"Successfully updated ticker summary: {ticker_summary.ticker}")
                    return ticker_summary
                
        except Exception as e:
            raise DatabaseQueryError("update ticker summary", str(e))
    
    def bulk_update(self, entities: List[TickerSummary]) -> int:
        """
        Update multiple ticker summary entries in a single transaction.
        
        Args:
            entities: List of TickerSummary entities to update
        
        Returns:
            Number of rows successfully updated
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        update_query = """
        UPDATE ticker_summary
        SET cik = %s, market_cap = %s, previous_close = %s, pe_ratio = %s,
            forward_pe_ratio = %s, dividend_yield = %s, payout_ratio = %s,
            fifty_day_average = %s, two_hundred_day_average = %s
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    data = [
                        (
                            ts.cik,
                            ts.market_cap,
                            ts.previous_close,
                            ts.pe_ratio,
                            ts.forward_pe_ratio,
                            ts.dividend_yield,
                            ts.payout_ratio,
                            ts.fifty_day_average,
                            ts.two_hundred_day_average,
                            ts.ticker
                        )
                        for ts in entities
                    ]
                    cursor.executemany(update_query, data)
                    rows_updated = cursor.rowcount
                    conn.commit()
                    self.logger.info(f"Successfully bulk updated {rows_updated} ticker summaries")
                    return rows_updated
                
        except Exception as e:
            raise DatabaseQueryError("bulk update ticker summaries", str(e))
    
    # ============================================================================
    # DELETE OPERATIONS
    # ============================================================================
    
    def delete(self, entity_id: str) -> bool:  # type: ignore[override]
        """
        Delete a ticker summary entry from the database.
        
        Args:
            entity_id: The ticker symbol to delete
        
        Returns:
            True if deleted, False if not found
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        delete_query = "DELETE FROM ticker_summary WHERE ticker = %s;"
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (entity_id.upper(),))
                    rows_deleted = cursor.rowcount
                    conn.commit()
                    
                    if rows_deleted > 0:
                        self.logger.info(f"Successfully deleted ticker summary: {entity_id}")
                        return True
                    else:
                        self.logger.warning(f"Ticker summary not found for deletion: {entity_id}")
                        return False
                
        except Exception as e:
            raise DatabaseQueryError("delete ticker summary", str(e))
    
    def bulk_delete(self, entity_ids: List[str]) -> int:  # type: ignore[override]
        """
        Delete multiple ticker summary entries in a single transaction.
        
        Args:
            entity_ids: List of ticker symbols to delete
        
        Returns:
            Number of rows successfully deleted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entity_ids:
            return 0
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['%s'] * len(entity_ids))
        delete_query = f"DELETE FROM ticker_summary WHERE ticker IN ({placeholders});"
        
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Convert all tickers to uppercase
                    upper_tickers = [ticker.upper() for ticker in entity_ids]
                    cursor.execute(delete_query, upper_tickers)  # type: ignore[arg-type]
                    rows_deleted = cursor.rowcount
                    conn.commit()
                    self.logger.info(f"Successfully bulk deleted {rows_deleted} ticker summaries")
                    return rows_deleted
                
        except Exception as e:
            raise DatabaseQueryError("bulk delete ticker summaries", str(e))
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    def _row_to_entity(self, row: tuple[Any, ...]) -> TickerSummary:
        """
        Convert a database row to a TickerSummary entity.
        
        Args:
            row: Database row tuple
        
        Returns:
            TickerSummary entity
        """
        return TickerSummary(
            ticker=row[0],
            cik=row[1],
            market_cap=row[2],
            previous_close=row[3],
            pe_ratio=row[4],
            forward_pe_ratio=row[5],
            dividend_yield=row[6],
            payout_ratio=row[7],
            fifty_day_average=row[8],
            two_hundred_day_average=row[9]
        )
