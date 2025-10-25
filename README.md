# Stock Information Database Updater

This project automatically updates a PostgreSQL database with stock ticker information and market capitalization data using Yahoo Query.

## Setup Instructions

### 1. Set up GitHub Environment and Secret

1. Go to your GitHub repository settings
2. Navigate to "Environments"
3. Create or select the environment named "GitHub Actions Secrets"
4. In the environment, go to "Environment secrets"
5. Click "Add secret"
6. Name: `DATABASE_URL`
7. Value: `[Your PostgreSQL connection string]`

**Note:** The workflow is configured to use the "GitHub Actions Secrets" environment, so make sure the environment name matches exactly.

### 2. Database Schema

**Important**: The `STOCKS` table must already exist in your database. The script will verify the table exists and has the expected structure before processing any data.

Required table structure:
```sql
CREATE TABLE STOCKS (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    company TEXT,
    exchange VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT STOCKS_pkey PRIMARY KEY (symbol)
);
```

The script will check for the existence of all required columns: `symbol` (primary key), `company`, `exchange`, `created_at`, and `last_updated_at`.

### 3. How it Works

The system now uses a **deterministic synchronization approach** that treats the ticker files as the source of truth:

#### Synchronization Strategy
1. **Scheduled Run**: The workflow runs daily at 7:00 AM UTC
2. **Manual Trigger**: You can manually trigger the workflow from the Actions tab
3. **Ticker Sources**: Downloads latest NYSE and NASDAQ ticker lists (source of truth)
4. **Three-Way Analysis**: Compares database state with ticker sources to determine:
   - **ADD**: Stocks in sources but not in database → Add after validation
   - **DELETE**: Stocks in database but not in sources → Remove from database  
   - **UPDATE**: Stocks in both but with different data → Update with latest info

#### Data Operations
- **Adding New Stocks**: Validates via Yahoo Finance API (checks for valid market cap)
- **Updating Existing Stocks**: Compares company names and exchange info with Yahoo data
- **Deleting Removed Stocks**: Removes stocks no longer listed in source exchanges
- **Timestamp Management**: Updates `last_updated_at` for all processed stocks
- **Exchange Detection**: Automatically detects NYSE/NASDAQ from filename

#### Key Features
- **Deterministic**: Same inputs always produce same database state
- **Data Integrity**: Yahoo Finance validation prevents invalid tickers
- **Efficient**: Batch processing with API rate limiting
- **Comprehensive**: Handles additions, updates, and deletions in single run

### 4. Local Development

To run the script locally:

```bash
# Set environment variable (replace with your actual connection string)
export DATABASE_URL="postgresql://username:password@host:port/database?sslmode=require"

# Install dependencies
cd Scripts
pip install -r requirements.txt

# Run with ticker files
python create_stocks_table.py ticker_file1.txt ticker_file2.txt
```

### 5. Features

- **Optimized Batch Processing**: Processes 50 symbols per batch with 6 concurrent workers (~8 symbols per worker)
- **Rate Limit Compliance**: Conservative batch/worker ratio prevents API throttling
- **Progress Tracking**: Shows progress bars during large batch operations
- **Error Handling**: Comprehensive logging, retry logic, and fallback mechanisms
- **Data Validation**: Only processes symbols with valid market cap data  
- **Exchange Detection**: Automatically identifies NYSE/NASDAQ from filenames
- **Duplicate Prevention**: Uses database constraints to prevent duplicate symbols
- **Efficient Updates**: Uses PostgreSQL's ON CONFLICT clause for upserts
- **API Rate Limiting**: Includes delays between batches to respect API limits
- **Caching**: GitHub Actions caches Python virtual environment for faster runs

### 6. Monitoring

Check the GitHub Actions logs to monitor:
- Number of symbols processed
- Success/failure rates  
- Database connection status
- Exchange assignment accuracy
- Any errors or warnings