"""
Ticker overview repository for database operations.
"""

import logging
import psycopg
from typing import List, Optional, Any

from .base_repository import BaseRepository
from ..models.ticker_overview import TickerOverview
from ..database.connection_manager import DatabaseConnectionManager
from ..exceptions import DatabaseQueryError


class TickerOverviewNotFoundError(Exception):
    """Exception raised when a ticker overview is not found."""
    
    def __init__(self, identifier: str, value: Any):
        self.identifier = identifier
        self.value = value
        super().__init__(f"Ticker overview not found by {identifier}: {value}")


class DuplicateTickerError(Exception):
    """Exception raised when attempting to create a duplicate ticker."""
    
    def __init__(self, ticker: str):
        self.ticker = ticker
        super().__init__(f"Ticker overview already exists: {ticker}")


class TickerOverviewRepository(BaseRepository[TickerOverview]):
    """
    Repository for ticker overview entities with full CRUD operations.
    Organized by: CREATE, READ, UPDATE, DELETE operations.
    Supports searching by ticker (primary key) and filtering by various metrics.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the ticker overview repository.
        
        Args:
            db_manager: Database connection manager instance
        """
        super().__init__(db_manager)
        self.logger = logging.getLogger(__name__)
        self.table_name = "ticker_overview"
    
    # ============================================================================
    # CREATE OPERATIONS
    # ============================================================================
    
    def insert(self, entity: TickerOverview) -> TickerOverview:
        """
        Create a new ticker overview entry in the database.
        
        Args:
            entity: TickerOverview entity to create
        
        Returns:
            Created ticker overview
        
        Raises:
            DuplicateTickerError: If ticker already exists
            DatabaseQueryError: If database operation fails
        """
        ticker_overview = entity
        
        # Check if ticker already exists
        if self.get_by_ticker(ticker_overview.ticker) is not None:
            raise DuplicateTickerError(ticker_overview.ticker)
        
        insert_query = """
        INSERT INTO ticker_overview (
            ticker, enterprise_to_ebitda, price_to_book, gross_margin,
            operating_margin, profit_margin, earnings_growth, revenue_growth,
            trailing_eps, forward_eps, peg_ratio, ebitda_margin
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(
                    insert_query,
                    (
                        ticker_overview.ticker,
                        ticker_overview.enterprise_to_ebitda,
                        ticker_overview.price_to_book,
                        ticker_overview.gross_margin,
                        ticker_overview.operating_margin,
                        ticker_overview.profit_margin,
                        ticker_overview.earnings_growth,
                        ticker_overview.revenue_growth,
                        ticker_overview.trailing_eps,
                        ticker_overview.forward_eps,
                        ticker_overview.peg_ratio,
                        ticker_overview.ebitda_margin
                    )
                )
                self.logger.info(f"Successfully inserted ticker overview: {ticker_overview.ticker}")
                return ticker_overview

        except psycopg.errors.UniqueViolation:
            raise DuplicateTickerError(ticker_overview.ticker)
        except Exception as e:
            raise DatabaseQueryError("insert ticker overview", str(e))
    
    def bulk_insert(self, entities: List[TickerOverview]) -> int:
        """
        Insert multiple ticker overview entries in a single transaction.
        Skips entries that already exist (uses ON CONFLICT DO NOTHING).
        
        Args:
            entities: List of TickerOverview entities to insert
        
        Returns:
            Number of rows successfully inserted
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        insert_query = """
        INSERT INTO ticker_overview (
            ticker, enterprise_to_ebitda, price_to_book, gross_margin,
            operating_margin, profit_margin, earnings_growth, revenue_growth,
            trailing_eps, forward_eps, peg_ratio, ebitda_margin
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO NOTHING;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                data = [
                    (
                        to.ticker,
                        to.enterprise_to_ebitda,
                        to.price_to_book,
                        to.gross_margin,
                        to.operating_margin,
                        to.profit_margin,
                        to.earnings_growth,
                        to.revenue_growth,
                        to.trailing_eps,
                        to.forward_eps,
                        to.peg_ratio,
                        to.ebitda_margin
                    )
                    for to in entities
                ]
                cursor.executemany(insert_query, data)
                rows_inserted = cursor.rowcount
                self.logger.info(f"Successfully bulk inserted {rows_inserted} ticker overviews")
                return rows_inserted

        except Exception as e:
            raise DatabaseQueryError("bulk insert ticker overviews", str(e))
    
    # ============================================================================
    # READ OPERATIONS
    # ============================================================================
    
    def get_by_ticker(self, ticker: str) -> Optional[TickerOverview]:
        """
        Retrieve a ticker overview entry by its ticker symbol (primary key).
        
        Args:
            ticker: The ticker symbol to retrieve
        
        Returns:
            TickerOverview if found, None otherwise
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        select_query = """
        SELECT ticker, enterprise_to_ebitda, price_to_book, gross_margin,
               operating_margin, profit_margin, earnings_growth, revenue_growth,
             trailing_eps, forward_eps, peg_ratio, ebitda_margin
        FROM ticker_overview
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, (ticker.upper(),))
                row = cursor.fetchone()

                if row is None:
                    return None

                return self._row_to_entity(row)

        except Exception as e:
            raise DatabaseQueryError("get ticker overview by ticker", str(e))
    
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[TickerOverview]:
        """
        Retrieve all ticker overview entries with optional pagination.
        
        Args:
            limit: Maximum number of entries to return
            offset: Number of entries to skip
        
        Returns:
            List of TickerOverview entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        query_parts = ["""
        SELECT ticker, enterprise_to_ebitda, price_to_book, gross_margin,
               operating_margin, profit_margin, earnings_growth, revenue_growth,
             trailing_eps, forward_eps, peg_ratio, ebitda_margin
        FROM ticker_overview
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
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, params)  # type: ignore[arg-type]
                rows = cursor.fetchall()

                return [self._row_to_entity(row) for row in rows]

        except Exception as e:
            raise DatabaseQueryError("get all ticker overviews", str(e))
    
    def count(self) -> int:
        """
        Count the total number of ticker overview entries.
        
        Returns:
            Total count of entries
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        count_query = "SELECT COUNT(*) FROM ticker_overview;"
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return result[0] if result else 0

        except Exception as e:
            raise DatabaseQueryError("count ticker overviews", str(e))
    
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
        query = "SELECT 1 FROM ticker_overview WHERE ticker = %s LIMIT 1;"
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, (ticker.upper(),))
                return cursor.fetchone() is not None

        except Exception as e:
            raise DatabaseQueryError("check ticker existence", str(e))
    
    # ============================================================================
    # UPDATE OPERATIONS
    # ============================================================================
    
    def update(self, entity: TickerOverview) -> TickerOverview:
        """
        Update an existing ticker overview entry in the database.
        
        Args:
            entity: TickerOverview entity to update (must have ticker as PK)
        
        Returns:
            Updated TickerOverview
        
        Raises:
            TickerOverviewNotFoundError: If ticker doesn't exist
            DatabaseQueryError: If database operation fails
        """
        ticker_overview = entity
        
        # Check if the ticker exists
        existing = self.get_by_ticker(ticker_overview.ticker)
        if existing is None:
            raise TickerOverviewNotFoundError("ticker", ticker_overview.ticker)
        
        update_query = """
        UPDATE ticker_overview
        SET enterprise_to_ebitda = %s, price_to_book = %s, gross_margin = %s,
            operating_margin = %s, profit_margin = %s, earnings_growth = %s,
            revenue_growth = %s, trailing_eps = %s, forward_eps = %s, peg_ratio = %s, ebitda_margin = %s
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(
                    update_query,
                    (
                        ticker_overview.enterprise_to_ebitda,
                        ticker_overview.price_to_book,
                        ticker_overview.gross_margin,
                        ticker_overview.operating_margin,
                        ticker_overview.profit_margin,
                        ticker_overview.earnings_growth,
                        ticker_overview.revenue_growth,
                        ticker_overview.trailing_eps,
                        ticker_overview.forward_eps,
                        ticker_overview.peg_ratio,
                        ticker_overview.ebitda_margin,
                        ticker_overview.ticker
                    )
                )
                self.logger.info(f"Successfully updated ticker overview: {ticker_overview.ticker}")
                return ticker_overview

        except Exception as e:
            raise DatabaseQueryError("update ticker overview", str(e))
    
    def bulk_update(self, entities: List[TickerOverview]) -> int:
        """
        Update multiple ticker overview entries in a single transaction.
        
        Args:
            entities: List of TickerOverview entities to update
        
        Returns:
            Number of rows successfully updated
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not entities:
            return 0
        
        update_query = """
        UPDATE ticker_overview
        SET enterprise_to_ebitda = %s, price_to_book = %s, gross_margin = %s,
            operating_margin = %s, profit_margin = %s, earnings_growth = %s,
            revenue_growth = %s, trailing_eps = %s, forward_eps = %s, peg_ratio = %s, ebitda_margin = %s
        WHERE ticker = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                data = [
                    (
                        to.enterprise_to_ebitda,
                        to.price_to_book,
                        to.gross_margin,
                        to.operating_margin,
                        to.profit_margin,
                        to.earnings_growth,
                        to.revenue_growth,
                        to.trailing_eps,
                        to.forward_eps,
                        to.peg_ratio,
                        to.ebitda_margin,
                        to.ticker
                    )
                    for to in entities
                ]
                cursor.executemany(update_query, data)
                rows_updated = cursor.rowcount
                self.logger.info(f"Successfully bulk updated {rows_updated} ticker overviews")
                return rows_updated

        except Exception as e:
            raise DatabaseQueryError("bulk update ticker overviews", str(e))
    
    # ============================================================================
    # DELETE OPERATIONS
    # ============================================================================
    
    def delete(self, entity_id: str) -> bool:  # type: ignore[override]
        """
        Delete a ticker overview entry from the database.
        
        Args:
            entity_id: The ticker symbol to delete
        
        Returns:
            True if deleted, False if not found
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        delete_query = "DELETE FROM ticker_overview WHERE ticker = %s;"
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(delete_query, (entity_id.upper(),))
                rows_deleted = cursor.rowcount

                if rows_deleted > 0:
                    self.logger.info(f"Successfully deleted ticker overview: {entity_id}")
                    return True
                else:
                    self.logger.warning(f"Ticker overview not found for deletion: {entity_id}")
                    return False

        except Exception as e:
            raise DatabaseQueryError("delete ticker overview", str(e))
    
    def bulk_delete(self, entity_ids: List[str]) -> int:  # type: ignore[override]
        """
        Delete multiple ticker overview entries in a single transaction.
        
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
        delete_query = f"DELETE FROM ticker_overview WHERE ticker IN ({placeholders});"
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                # Convert all tickers to uppercase
                upper_tickers = [ticker.upper() for ticker in entity_ids]
                cursor.execute(delete_query, upper_tickers)  # type: ignore[arg-type]
                rows_deleted = cursor.rowcount
                self.logger.info(f"Successfully bulk deleted {rows_deleted} ticker overviews")
                return rows_deleted

        except Exception as e:
            raise DatabaseQueryError("bulk delete ticker overviews", str(e))
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    def _row_to_entity(self, row: tuple[Any, ...]) -> TickerOverview:
        """
        Convert a database row to a TickerOverview entity.
        
        Args:
            row: Database row tuple
        
        Returns:
            TickerOverview entity
        """
        return TickerOverview(
            ticker=row[0],
            enterprise_to_ebitda=row[1],
            price_to_book=row[2],
            gross_margin=row[3],
            operating_margin=row[4],
            profit_margin=row[5],
            earnings_growth=row[6],
            revenue_growth=row[7],
            trailing_eps=row[8],
            forward_eps=row[9],
            peg_ratio=row[10],
            ebitda_margin=row[11]
        )
