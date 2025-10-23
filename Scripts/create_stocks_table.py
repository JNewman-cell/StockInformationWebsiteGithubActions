import yahooquery as yq
import pandas as pd
import psycopg2
import sys
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_connection(connection_string):
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(connection_string)
        logger.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def create_stocks_table(conn):
    """Create the stocks table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stocks (
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        symbol VARCHAR(20) UNIQUE NOT NULL,
        company TEXT,
        exchange VARCHAR(10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_symbol UNIQUE (symbol),
        CONSTRAINT STOCKS_pkey PRIMARY KEY (id)
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        logger.info("Stocks table created or already exists")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_ticker_info(ticker, exchange=None):
    """Fetch basic info for a single ticker using yahooquery."""
    try:
        stock = yq.Ticker(ticker)
        
        # Get basic info
        info = stock.summary_detail
        profile = stock.asset_profile
        
        if ticker not in info or info[ticker] is None:
            logger.warning(f"No data available for ticker: {ticker}")
            return None
            
        ticker_info = info[ticker]
        
        # Extract market cap to validate the ticker has data
        market_cap = ticker_info.get('marketCap')
        if market_cap is None or market_cap == 0:
            logger.warning(f"No market cap available for ticker: {ticker}")
            return None
            
        # Get company name
        company_name = None
        if ticker in profile and profile[ticker] is not None:
            profile_data = profile[ticker]
            company_name = profile_data.get('longName')
        
        return {
            'symbol': ticker,
            'company': company_name,
            'exchange': exchange,
            'market_cap': market_cap  # We'll use this for validation but not store it
        }
        
    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return None

def insert_or_update_ticker(conn, ticker_data):
    """Insert or update ticker data in the database."""
    insert_query = """
    INSERT INTO stocks (symbol, company, exchange, created_at, last_updated_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (symbol) 
    DO UPDATE SET 
        company = EXCLUDED.company,
        exchange = EXCLUDED.exchange,
        last_updated_at = EXCLUDED.last_updated_at;
    """
    
    try:
        cursor = conn.cursor()
        current_time = datetime.now()
        cursor.execute(insert_query, (
            ticker_data['symbol'],
            ticker_data['company'],
            ticker_data['exchange'],
            current_time,
            current_time
        ))
        conn.commit()
        cursor.close()
        logger.info(f"Successfully inserted/updated symbol: {ticker_data['symbol']}")
        return True
    except Exception as e:
        logger.error(f"Error inserting/updating symbol {ticker_data['symbol']}: {e}")
        return False

def process_ticker_file(file_path):
    """Process a ticker file and return list of tickers with exchange info."""
    tickers = []
    
    # Determine exchange from filename
    exchange = None
    if 'nyse' in file_path.lower():
        exchange = 'NYSE'
    elif 'nasdaq' in file_path.lower():
        exchange = 'NASDAQ'
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                ticker = line.strip().upper()
                if ticker and len(ticker) <= 20:  # Basic validation for symbol length
                    tickers.append((ticker, exchange))
        logger.info(f"Loaded {len(tickers)} tickers from {file_path} (exchange: {exchange})")
        return tickers
    except Exception as e:
        logger.error(f"Error reading ticker file {file_path}: {e}")
        return []

def main():
    """Main function to process tickers and update database."""
    if len(sys.argv) < 2:
        logger.error("Usage: python create_stocks_table.py <ticker_file1> [ticker_file2] ...")
        sys.exit(1)
    
    # Get database connection string from environment
    connection_string = os.getenv('DATABASE_URL')
    if not connection_string:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Connect to database
    conn = create_database_connection(connection_string)
    if not conn:
        sys.exit(1)
    
    # Create table
    if not create_stocks_table(conn):
        conn.close()
        sys.exit(1)
    
    # Process all ticker files
    all_tickers = []
    for file_path in sys.argv[1:]:
        if os.path.exists(file_path):
            tickers = process_ticker_file(file_path)
            all_tickers.extend(tickers)
        else:
            logger.warning(f"Ticker file not found: {file_path}")
    
    # Remove duplicates (keep first occurrence with exchange info)
    seen_symbols = set()
    unique_tickers = []
    for symbol, exchange in all_tickers:
        if symbol not in seen_symbols:
            unique_tickers.append((symbol, exchange))
            seen_symbols.add(symbol)
    
    logger.info(f"Processing {len(unique_tickers)} unique tickers")
    
    # Process each ticker
    successful_updates = 0
    failed_updates = 0
    
    for symbol, exchange in unique_tickers:
        logger.info(f"Processing symbol: {symbol} (exchange: {exchange})")
        
        # Get ticker data (validates market cap exists)
        ticker_data = get_ticker_info(symbol, exchange)
        
        if ticker_data:
            # Insert/update in database
            if insert_or_update_ticker(conn, ticker_data):
                successful_updates += 1
            else:
                failed_updates += 1
        else:
            logger.warning(f"Skipping symbol {symbol} - no market cap data available")
            failed_updates += 1
    
    # Close database connection
    conn.close()
    
    logger.info(f"Processing complete. Successful: {successful_updates}, Failed: {failed_updates}")
    print(f"Processing complete. Successfully processed {successful_updates} symbols.")

if __name__ == "__main__":
    main()