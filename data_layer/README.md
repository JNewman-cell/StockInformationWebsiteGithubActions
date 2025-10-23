# Stock Information Data Layer

A comprehensive data access layer for stock information management with full CRUD capabilities, built for PostgreSQL databases.

## Features

- **Full CRUD Operations**: Create, Read, Update, Delete operations for stocks
- **Connection Pooling**: Efficient database connection management with pooling
- **Data Validation**: Comprehensive input validation and sanitization
- **Error Handling**: Custom exceptions for different error scenarios
- **Search Functionality**: Advanced search capabilities with filtering
- **Bulk Operations**: Efficient bulk insert and update operations
- **Type Safety**: Full type hints and dataclass-based models

## Project Structure

```
data_layer/
├── __init__.py              # Package initialization and exports
├── exceptions.py            # Custom exception classes
├── example_usage.py         # Usage examples and demonstrations
├── test_data_layer.py       # Comprehensive test suite
├── database/
│   ├── __init__.py
│   └── connection_manager.py # Database connection management
├── models/
│   ├── __init__.py
│   └── stock.py             # Stock data model
└── repositories/
    ├── __init__.py
    ├── base_repository.py    # Abstract base repository
    └── stock_repository.py   # Stock-specific repository
```

## Quick Start

### 1. Prerequisites

Ensure you have the required dependencies installed:

```bash
pip install psycopg2-binary pandas
```

### 2. Environment Setup

Set your database connection string as an environment variable:

```bash
set DATABASE_URL=postgresql://username:password@localhost:5432/dbname
```

Or on Unix/Linux:
```bash
export DATABASE_URL=postgresql://username:password@localhost:5432/dbname
```

### 3. Basic Usage

```python
from data_layer import DatabaseConnectionManager, StockRepository, Stock

# Initialize database connection
db_manager = DatabaseConnectionManager()

# Initialize repository
stock_repo = StockRepository(db_manager)

# Create a new stock
stock = Stock(
    symbol="AAPL",
    company="Apple Inc.",
    exchange="NASDAQ"
)

created_stock = stock_repo.create(stock)
print(f"Created stock: {created_stock}")

# Retrieve stock by symbol
retrieved_stock = stock_repo.get_by_symbol("AAPL")
print(f"Retrieved: {retrieved_stock}")

# Update stock
retrieved_stock.company = "Apple Inc. (Updated)"
updated_stock = stock_repo.update(retrieved_stock)

# Search stocks
nasdaq_stocks = stock_repo.get_by_exchange("NASDAQ", limit=10)
print(f"Found {len(nasdaq_stocks)} NASDAQ stocks")

# Clean up
db_manager.close_all_connections()
```

## Detailed Usage Guide

### Database Connection Management

The `DatabaseConnectionManager` handles database connections with connection pooling:

```python
from data_layer import DatabaseConnectionManager

# Initialize with default settings
db_manager = DatabaseConnectionManager()

# Initialize with custom connection string
db_manager = DatabaseConnectionManager(
    connection_string="postgresql://user:pass@localhost/db",
    min_connections=2,
    max_connections=20
)

# Test connection
if db_manager.test_connection():
    print("Database connection successful!")

# Use context manager for manual connection handling
with db_manager.get_connection_context() as conn:
    # Use connection directly
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()

# Use cursor context manager (recommended)
with db_manager.get_cursor_context() as cursor:
    cursor.execute("SELECT COUNT(*) FROM STOCKS")
    count = cursor.fetchone()[0]
```

### Stock Model

The `Stock` model provides data validation and convenient methods:

```python
from data_layer import Stock, ValidationError

# Create a valid stock
stock = Stock(
    symbol="MSFT",
    company="Microsoft Corporation",
    exchange="NASDAQ"
)

# Validation is automatic
try:
    invalid_stock = Stock(symbol="")  # Empty symbol
except ValidationError as e:
    print(f"Validation error: {e}")

# Convert to/from dictionary
stock_dict = stock.to_dict()
stock_from_dict = Stock.from_dict(stock_dict)

# Update from dictionary
stock.update_from_dict({
    'company': 'Microsoft Corp.',
    'exchange': 'NASDAQ'
})
```

### Stock Repository Operations

#### Create Operations

```python
from data_layer import StockRepository, Stock, DuplicateStockError

# Single stock creation
stock = Stock(symbol="GOOGL", company="Alphabet Inc.", exchange="NASDAQ")

try:
    created_stock = stock_repo.create(stock)
    print(f"Created: {created_stock.symbol} with ID {created_stock.id}")
except DuplicateStockError as e:
    print(f"Stock already exists: {e}")

# Bulk insert
stocks = [
    Stock(symbol="AMZN", company="Amazon.com Inc.", exchange="NASDAQ"),
    Stock(symbol="TSLA", company="Tesla Inc.", exchange="NASDAQ"),
    Stock(symbol="META", company="Meta Platforms Inc.", exchange="NASDAQ")
]

created_stocks = stock_repo.bulk_insert(stocks)
print(f"Bulk inserted {len(created_stocks)} stocks")
```

#### Read Operations

```python
# Get by ID
stock = stock_repo.get_by_id(123)

# Get by symbol (case-insensitive)
stock = stock_repo.get_by_symbol("AAPL")

# Get all stocks with pagination
all_stocks = stock_repo.get_all(limit=100, offset=0)

# Get stocks by exchange
nasdaq_stocks = stock_repo.get_by_exchange("NASDAQ")

# Advanced search
search_results = stock_repo.search(
    symbol_pattern="A%",      # Symbols starting with 'A'
    company_pattern="Apple",   # Companies containing 'Apple'
    exchange="NASDAQ",        # Exact exchange match
    limit=50
)

# Count operations
total_stocks = stock_repo.count()
print(f"Total stocks in database: {total_stocks}")

# Get available exchanges
exchanges = stock_repo.get_exchanges()
print(f"Available exchanges: {exchanges}")
```

#### Update Operations

```python
from data_layer import StockNotFoundError

# Update existing stock
stock = stock_repo.get_by_symbol("AAPL")
if stock:
    stock.company = "Apple Inc. (Updated)"
    stock.exchange = "NASDAQ"
    
    try:
        updated_stock = stock_repo.update(stock)
        print(f"Updated: {updated_stock}")
    except StockNotFoundError as e:
        print(f"Stock not found: {e}")
```

#### Delete Operations

```python
# Delete by ID
success = stock_repo.delete(123)
if success:
    print("Stock deleted successfully")

# Delete by symbol
success = stock_repo.delete_by_symbol("AAPL")
if success:
    print("Stock deleted by symbol")
```

## Error Handling

The data layer provides comprehensive error handling with custom exceptions:

```python
from data_layer import (
    DataLayerError,
    DatabaseConnectionError,
    StockNotFoundError,
    DuplicateStockError,
    ValidationError,
    DatabaseQueryError
)

try:
    # Your data layer operations
    stock = stock_repo.create(new_stock)
    
except ValidationError as e:
    print(f"Invalid data: {e}")
    
except DuplicateStockError as e:
    print(f"Stock already exists: {e}")
    
except StockNotFoundError as e:
    print(f"Stock not found: {e}")
    
except DatabaseConnectionError as e:
    print(f"Database connection failed: {e}")
    
except DatabaseQueryError as e:
    print(f"Database query failed: {e}")
    
except DataLayerError as e:
    print(f"Data layer error: {e}")
```

## Testing

The data layer includes a comprehensive test suite:

```bash
# Run tests (requires DATABASE_URL environment variable)
python data_layer/test_data_layer.py

# Run example usage
python data_layer/example_usage.py
```

### Test Coverage

- Stock model validation
- Database connection management
- All CRUD operations
- Search functionality
- Bulk operations
- Error handling scenarios
- Edge cases and boundary conditions

## Database Schema Requirements

The data layer expects a PostgreSQL table with the following structure:

```sql
CREATE TABLE STOCKS (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    company VARCHAR(255),
    exchange VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stocks_symbol ON STOCKS(symbol);
CREATE INDEX idx_stocks_exchange ON STOCKS(exchange);
CREATE INDEX idx_stocks_company ON STOCKS(company);
```

## Configuration Options

### Database Connection Manager

- `connection_string`: PostgreSQL connection string
- `min_connections`: Minimum connections in pool (default: 1)
- `max_connections`: Maximum connections in pool (default: 10)

### Stock Repository

- Supports case-insensitive symbol lookups
- Automatic timestamp management
- Conflict resolution for bulk operations
- Configurable pagination limits

## Best Practices

1. **Connection Management**: Always use context managers or ensure connections are properly closed
2. **Error Handling**: Catch specific exceptions rather than generic ones
3. **Validation**: Let the model handle validation; don't bypass it
4. **Bulk Operations**: Use bulk_insert for multiple records to improve performance
5. **Search Optimization**: Use indexed fields (symbol, exchange) for better performance

## Performance Considerations

- Connection pooling reduces connection overhead
- Bulk operations are optimized for large datasets
- Proper indexing on searchable fields
- Lazy loading and pagination support for large result sets

## Integration with Existing Code

The data layer can be easily integrated with your existing stock management script:

```python
# In your existing create_stocks_table.py
from data_layer import DatabaseConnectionManager, StockRepository, Stock

def main():
    # Replace direct database operations with data layer
    db_manager = DatabaseConnectionManager()
    stock_repo = StockRepository(db_manager)
    
    # Convert your ticker processing to use the data layer
    for ticker_data in all_ticker_data:
        stock = Stock(
            symbol=ticker_data['symbol'],
            company=ticker_data['company'],
            exchange=ticker_data['exchange']
        )
        
        try:
            created_stock = stock_repo.create(stock)
            successful_updates += 1
        except DuplicateStockError:
            # Handle duplicate by updating
            existing_stock = stock_repo.get_by_symbol(stock.symbol)
            existing_stock.update_from_dict(ticker_data)
            stock_repo.update(existing_stock)
            successful_updates += 1
        except Exception as e:
            logger.error(f"Error processing {stock.symbol}: {e}")
            failed_updates += 1
    
    db_manager.close_all_connections()
```

## Contributing

To extend the data layer:

1. Follow the existing patterns for new models and repositories
2. Inherit from `BaseRepository` for new entity repositories
3. Add comprehensive validation to new models
4. Include proper error handling and logging
5. Write tests for new functionality

## License

This data layer is part of the Stock Information Website project and follows the same licensing terms.