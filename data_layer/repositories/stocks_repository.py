"""
Stock repository for database operations.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

from .base_repository import BaseRepository
from ..models.stock import Stock
from ..database.connection_manager import DatabaseConnectionManager
from ..exceptions import (
    StockNotFoundError,
    DuplicateStockError,
    DatabaseQueryError,
    ValidationError
)


class StocksRepository(BaseRepository[Stock]):
    """
    Repository for Stock entities with full CRUD operations.
    """
    
    def __init__(self, db_manager: DatabaseConnectionManager):
        """
        Initialize the stock repository.
        
        Args:
            db_manager: Database connection manager instance
        """
        super().__init__(db_manager)
        self.logger = logging.getLogger(__name__)
        self.table_name = "STOCKS"
    
    def create(self, stock: Stock) -> Stock:
        """
        Create a new stock in the database.
        
        Args:
            stock: Stock entity to create
        
        Returns:
            Created stock with ID and timestamps
        
        Raises:
            DuplicateStockError: If stock symbol already exists
            DatabaseQueryError: If database operation fails
        """
        # Validate the stock
        stock.validate()
        
        # Check if stock already exists
        if self.get_by_symbol(stock.symbol) is not None:
            raise DuplicateStockError(stock.symbol)
        
        insert_query = """
        INSERT INTO "STOCKS" (symbol, company, exchange, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, created_at, last_updated_at;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                cursor.execute(insert_query, (
                    stock.symbol,
                    stock.company,
                    stock.exchange,
                    current_time,
                    current_time
                ))
                
                result = cursor.fetchone()
                
                # Update the stock object with database-generated values
                stock.id = result[0]
                stock.created_at = result[1]
                stock.last_updated_at = result[2]
                
                self.logger.info(f"Created stock: {stock.symbol} (ID: {stock.id})")
                return stock
                
        except psycopg2.IntegrityError as e:
            raise DuplicateStockError(stock.symbol)
        except Exception as e:
            raise DatabaseQueryError("create stock", str(e))
    
    def get_by_id(self, stock_id: int) -> Optional[Stock]:
        """
        Retrieve a stock by its ID.
        
        Args:
            stock_id: The ID of the stock to retrieve
        
        Returns:
            Stock if found, None otherwise
        """
        select_query = """
        SELECT id, symbol, company, exchange, created_at, last_updated_at
        FROM "STOCKS"
        WHERE id = %s;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, (stock_id,))
                result = cursor.fetchone()
                
                if result:
                    return Stock(
                        id=result[0],
                        symbol=result[1],
                        company=result[2],
                        exchange=result[3],
                        created_at=result[4],
                        last_updated_at=result[5]
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving stock by ID {stock_id}: {e}")
            raise DatabaseQueryError("get stock by ID", str(e))
    
    def get_by_symbol(self, symbol: str) -> Optional[Stock]:
        """
        Retrieve a stock by its symbol.
        
        Args:
            symbol: The symbol of the stock to retrieve
        
        Returns:
            Stock if found, None otherwise
        """
        select_query = """
        SELECT id, symbol, company, exchange, created_at, last_updated_at
        FROM "STOCKS"
        WHERE UPPER(symbol) = UPPER(%s);
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(select_query, (symbol.strip(),))
                result = cursor.fetchone()
                
                if result:
                    return Stock(
                        id=result[0],
                        symbol=result[1],
                        company=result[2],
                        exchange=result[3],
                        created_at=result[4],
                        last_updated_at=result[5]
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving stock by symbol {symbol}: {e}")
            raise DatabaseQueryError("get stock by symbol", str(e))
    
    def update(self, stock: Stock) -> Stock:
        """
        Update an existing stock in the database.
        
        Args:
            stock: Stock entity to update (must have ID)
        
        Returns:
            Updated stock
        
        Raises:
            StockNotFoundError: If stock doesn't exist
            ValidationError: If stock ID is missing
            DatabaseQueryError: If database operation fails
        """
        if stock.id is None:
            raise ValidationError("id", stock.id, "Stock ID is required for update")
        
        # Validate the stock
        stock.validate()
        
        # Check if stock exists
        existing_stock = self.get_by_id(stock.id)
        if existing_stock is None:
            raise StockNotFoundError(str(stock.id), "ID")
        
        update_query = """
        UPDATE "STOCKS" 
        SET symbol = %s, company = %s, exchange = %s, last_updated_at = %s
        WHERE id = %s
        RETURNING last_updated_at;
        """
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                cursor.execute(update_query, (
                    stock.symbol,
                    stock.company,
                    stock.exchange,
                    current_time,
                    stock.id
                ))
                
                result = cursor.fetchone()
                stock.last_updated_at = result[0]
                
                self.logger.info(f"Updated stock: {stock.symbol} (ID: {stock.id})")
                return stock
                
        except psycopg2.IntegrityError as e:
            if "unique" in str(e).lower():
                raise DuplicateStockError(stock.symbol)
            raise DatabaseQueryError("update stock", str(e))
        except Exception as e:
            raise DatabaseQueryError("update stock", str(e))
    
    def delete(self, stock_id: int) -> bool:
        """
        Delete a stock by its ID.
        
        Args:
            stock_id: The ID of the stock to delete
        
        Returns:
            True if deletion was successful, False if stock not found
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        delete_query = 'DELETE FROM "STOCKS" WHERE id = %s;'
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(delete_query, (stock_id,))
                deleted_count = cursor.rowcount
                
                if deleted_count > 0:
                    self.logger.info(f"Deleted stock with ID: {stock_id}")
                    return True
                else:
                    self.logger.warning(f"No stock found with ID: {stock_id}")
                    return False
                    
        except Exception as e:
            raise DatabaseQueryError("delete stock", str(e))
    
    def delete_by_symbol(self, symbol: str) -> bool:
        """
        Delete a stock by its symbol.
        
        Args:
            symbol: The symbol of the stock to delete
        
        Returns:
            True if deletion was successful, False if stock not found
        """
        delete_query = 'DELETE FROM "STOCKS" WHERE UPPER(symbol) = UPPER(%s);'
        
        try:
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(delete_query, (symbol.strip(),))
                deleted_count = cursor.rowcount
                
                if deleted_count > 0:
                    self.logger.info(f"Deleted stock with symbol: {symbol}")
                    return True
                else:
                    self.logger.warning(f"No stock found with symbol: {symbol}")
                    return False
                    
        except Exception as e:
            raise DatabaseQueryError("delete stock by symbol", str(e))
    
    def get_all(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Stock]:
        """
        Retrieve all stocks with optional pagination.
        
        Args:
            limit: Maximum number of stocks to return
            offset: Number of stocks to skip
        
        Returns:
            List of stocks
        """
        query = """
        SELECT id, symbol, company, exchange, created_at, last_updated_at
        FROM "STOCKS"
        ORDER BY symbol
        """
        
        params = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        if offset is not None:
            query += " OFFSET %s"
            params.append(offset)
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                stocks = []
                for row in results:
                    stock = Stock(
                        id=row[0],
                        symbol=row[1],
                        company=row[2],
                        exchange=row[3],
                        created_at=row[4],
                        last_updated_at=row[5]
                    )
                    stocks.append(stock)
                
                return stocks
                
        except Exception as e:
            raise DatabaseQueryError("get all stocks", str(e))
    
    def search(self, 
              symbol_pattern: Optional[str] = None,
              company_pattern: Optional[str] = None,
              exchange: Optional[str] = None,
              limit: Optional[int] = None,
              offset: Optional[int] = None) -> List[Stock]:
        """
        Search stocks with various filters.
        
        Args:
            symbol_pattern: Pattern to match against symbol (supports SQL LIKE patterns)
            company_pattern: Pattern to match against company name (supports SQL LIKE patterns)
            exchange: Exact exchange to match
            limit: Maximum number of results
            offset: Number of results to skip
        
        Returns:
            List of matching stocks
        """
        conditions = []
        params = []
        
        if symbol_pattern:
            conditions.append("UPPER(symbol) LIKE UPPER(%s)")
            params.append(f"%{symbol_pattern}%")
        
        if company_pattern:
            conditions.append("UPPER(company) LIKE UPPER(%s)")
            params.append(f"%{company_pattern}%")
        
        if exchange:
            conditions.append("UPPER(exchange) = UPPER(%s)")
            params.append(exchange)
        
        query = """
        SELECT id, symbol, company, exchange, created_at, last_updated_at
        FROM "STOCKS"
        """
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY symbol"
        
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        if offset is not None:
            query += " OFFSET %s"
            params.append(offset)
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                stocks = []
                for row in results:
                    stock = Stock(
                        id=row[0],
                        symbol=row[1],
                        company=row[2],
                        exchange=row[3],
                        created_at=row[4],
                        last_updated_at=row[5]
                    )
                    stocks.append(stock)
                
                return stocks
                
        except Exception as e:
            raise DatabaseQueryError("search stocks", str(e))
    
    def get_by_exchange(self, exchange: str, limit: Optional[int] = None) -> List[Stock]:
        """
        Get stocks by exchange.
        
        Args:
            exchange: Exchange name
            limit: Maximum number of results
        
        Returns:
            List of stocks from the specified exchange
        """
        return self.search(exchange=exchange, limit=limit)
    
    def count(self) -> int:
        """
        Count the total number of stocks.
        
        Returns:
            Total count of stocks
        """
        count_query = 'SELECT COUNT(*) FROM "STOCKS";'
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(count_query)
                result = cursor.fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            raise DatabaseQueryError("count stocks", str(e))
    
    def get_exchanges(self) -> List[str]:
        """
        Get list of all exchanges in the database.
        
        Returns:
            List of unique exchange names
        """
        query = """
        SELECT DISTINCT exchange 
        FROM "STOCKS" 
        WHERE exchange IS NOT NULL 
        ORDER BY exchange;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                return [row[0] for row in results]
                
        except Exception as e:
            raise DatabaseQueryError("get exchanges", str(e))
    
    def get_all_symbols(self) -> Dict[str, Stock]:
        """
        Get all symbols from the database as a dictionary for efficient lookup.
        
        Returns:
            Dictionary mapping symbol to Stock object
        """
        query = """
        SELECT id, symbol, company, exchange, created_at, last_updated_at
        FROM "STOCKS"
        ORDER BY symbol;
        """
        
        try:
            with self.db_manager.get_cursor_context(commit=False) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                symbols_dict = {}
                for row in results:
                    stock = Stock(
                        id=row[0],
                        symbol=row[1],
                        company=row[2],
                        exchange=row[3],
                        created_at=row[4],
                        last_updated_at=row[5]
                    )
                    symbols_dict[stock.symbol] = stock
                
                self.logger.info(f"Retrieved {len(symbols_dict)} symbols from database")
                return symbols_dict
                
        except Exception as e:
            raise DatabaseQueryError("get all symbols", str(e))
    
    def bulk_insert(self, stocks: List[Stock]) -> List[Stock]:
        """
        Insert multiple stocks in a single transaction.
        
        Args:
            stocks: List of stock entities to insert
        
        Returns:
            List of created stocks with IDs and timestamps
        
        Raises:
            ValidationError: If any stock is invalid
            DatabaseQueryError: If database operation fails
        """
        if not stocks:
            return []
        
        # Validate all stocks first
        for stock in stocks:
            stock.validate()
        
        insert_query = """
        INSERT INTO "STOCKS" (symbol, company, exchange, created_at, last_updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET
            company = EXCLUDED.company,
            exchange = EXCLUDED.exchange,
            last_updated_at = EXCLUDED.last_updated_at
        RETURNING id, symbol, created_at, last_updated_at;
        """
        
        try:
            created_stocks = []
            
            with self.db_manager.get_cursor_context() as cursor:
                current_time = datetime.now()
                
                for stock in stocks:
                    cursor.execute(insert_query, (
                        stock.symbol,
                        stock.company,
                        stock.exchange,
                        current_time,
                        current_time
                    ))
                    
                    result = cursor.fetchone()
                    
                    # Create new stock object with database values
                    created_stock = Stock(
                        id=result[0],
                        symbol=result[1],
                        company=stock.company,
                        exchange=stock.exchange,
                        created_at=result[2],
                        last_updated_at=result[3]
                    )
                    created_stocks.append(created_stock)
            
            self.logger.info(f"Bulk inserted {len(created_stocks)} stocks")
            return created_stocks
            
        except Exception as e:
            raise DatabaseQueryError("bulk insert stocks", str(e))
    
    def bulk_update_timestamps(self, symbols: List[str]) -> int:
        """
        Update last_updated_at timestamp for multiple stocks by symbol in a single operation.
        
        Args:
            symbols: List of stock symbols to update timestamps for
        
        Returns:
            Number of stocks updated
        
        Raises:
            DatabaseQueryError: If database operation fails
        """
        if not symbols:
            return 0
        
        update_query = """
        UPDATE "STOCKS" 
        SET last_updated_at = %s 
        WHERE symbol = ANY(%s);
        """
        
        try:
            current_time = datetime.now()
            
            with self.db_manager.get_cursor_context() as cursor:
                cursor.execute(update_query, (current_time, symbols))
                updated_count = cursor.rowcount
                
                self.logger.info(f"Bulk updated timestamps for {updated_count} stocks")
                return updated_count
                
        except Exception as e:
            raise DatabaseQueryError("bulk update timestamps", str(e))
    
    def bulk_update_stocks(self, stocks: List[Stock]) -> int:
        """
        Update multiple stocks in a single transaction.
        
        Args:
            stocks: List of Stock entities to update
        
        Returns:
            Number of stocks updated
        
        Raises:
            ValidationError: If any stock is invalid
            DatabaseQueryError: If database operation fails
        """
        if not stocks:
            return 0
        
        # Validate all stocks first
        for stock in stocks:
            stock.validate()
        
        update_query = """
        UPDATE "STOCKS" 
        SET company = %s, exchange = %s, last_updated_at = %s
        WHERE id = %s;
        """
        
        try:
            current_time = datetime.now()
            updated_count = 0
            
            with self.db_manager.get_cursor_context() as cursor:
                for stock in stocks:
                    # Ensure last_updated_at is set
                    if stock.last_updated_at is None or stock.last_updated_at == stock.created_at:
                        stock.last_updated_at = current_time
                    
                    cursor.execute(update_query, (
                        stock.company,
                        stock.exchange,
                        stock.last_updated_at,
                        stock.id
                    ))
                    updated_count += cursor.rowcount
                
                self.logger.info(f"Bulk updated {updated_count} stocks")
                return updated_count
                
        except Exception as e:
            raise DatabaseQueryError("bulk update stocks", str(e))