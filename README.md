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

**Important**: The `stocks` table must already exist in your database. The script will verify the table exists and has the expected structure before processing any data.

Required table structure:
```sql
CREATE TABLE stocks (
    id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    symbol VARCHAR(20) UNIQUE NOT NULL,
    company TEXT,
    exchange VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol UNIQUE (symbol),
    CONSTRAINT STOCKS_pkey PRIMARY KEY (id)
);
```

The script will check for the existence of all required columns: `id`, `symbol`, `company`, `exchange`, `created_at`, and `last_updated_at`.

### 3. How it Works

1. **Scheduled Run**: The workflow runs daily at 7:00 AM UTC
2. **Manual Trigger**: You can manually trigger the workflow from the Actions tab
3. **Ticker Sources**: Downloads latest NYSE and NASDAQ ticker lists
3. **Data Validation**: Only adds symbols that have valid market cap data from Yahoo Finance
4. **Exchange Detection**: Automatically detects NYSE/NASDAQ from filename
5. **Upsert Logic**: Updates existing symbols or inserts new ones

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