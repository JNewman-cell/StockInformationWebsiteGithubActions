"""
Utility functions for CIK lookup table synchronization.
"""

import logging
import os
import sys
from typing import Dict, List, Tuple

# Add data layer to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import CikLookup

# Add entities to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from entities.synchronization_result import SynchronizationResult

logger = logging.getLogger(__name__)


def fetch_ticker_data_from_github_repo() -> List[str]:
    """Fetch ticker data directly from the Improved-US-Stock-Symbols GitHub repository.
    
    Uses the 'all_tickers.json' file which contains symbols from all exchanges.
    This is the same source used by the stocks table synchronization.
    
    Returns:
        List of normalized ticker symbols
    """
    import requests
    
    tickers = []
    
    # URL for the all tickers JSON file - contains symbols from all US exchanges
    url = 'https://raw.githubusercontent.com/JNewman-cell/Improved-US-Stock-Symbols/main/all/all_tickers.json'
    
    try:
        logger.info("Fetching all US ticker data from GitHub repository...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response - should be a simple array of ticker symbols
        ticker_symbols = response.json()
        
        if not isinstance(ticker_symbols, list):
            logger.error(f"Unexpected JSON format: expected list, got {type(ticker_symbols)}")
            raise RuntimeError("Invalid JSON format received from GitHub repository")
            
        # Process each ticker symbol
        filtered_count = 0
        for ticker in ticker_symbols:
            if isinstance(ticker, str) and ticker.strip():
                # Skip tickers with ^ character (preferred shares, warrants, etc.)
                if '^' in ticker:
                    filtered_count += 1
                    continue
                    
                # Normalize ticker by replacing / and \ with - to follow Yahoo Finance conventions
                normalized_ticker = ticker.strip().upper().replace('/', '-').replace('\\', '-')
                # Filter out tickers longer than 6 characters (likely invalid)
                if len(normalized_ticker) <= 6:
                    tickers.append(normalized_ticker)
        
        logger.info(f"Successfully loaded {len(tickers)} ticker symbols from GitHub repository")
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} tickers containing '^' character (preferred shares, warrants, etc.)")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching ticker data from GitHub: {e}")
        raise RuntimeError(f"Failed to fetch ticker data from GitHub repository: {e}")
    except ValueError as e:
        logger.error(f"Error parsing JSON response: {e}")
        raise RuntimeError(f"Failed to parse ticker data from GitHub repository: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching ticker data: {e}")
        raise RuntimeError(f"Unexpected error fetching ticker data: {e}")
    
    if not tickers:
        raise RuntimeError("No valid ticker symbols found in GitHub repository")
    
    return tickers


def lookup_cik_and_company_name_batch(tickers: List[str]) -> Tuple[Dict[str, Tuple[int, str]], List[str]]:
    """
    Lookup CIK and company name for multiple tickers using sec-company-lookup.
    
    Args:
        tickers: List of ticker symbols to lookup
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to (cik, company_name) tuples
        - List of tickers that failed lookup
    """
    from sec_company_lookup import get_companies_by_tickers
    
    results = {}
    failed_tickers = []
    
    try:
        # Use batch lookup for efficiency
        logger.info(f"Looking up CIK and company names for {len(tickers)} tickers...")
        batch_results = get_companies_by_tickers(tickers)
        
        for ticker in tickers:
            if ticker in batch_results:
                result = batch_results[ticker]
                
                if result.get('success') and result.get('data'):
                    company_data = result['data']
                    cik = company_data.get('cik')
                    name = company_data.get('name')
                    
                    if cik is not None and name:
                        results[ticker] = (cik, name)
                    else:
                        logger.debug(f"Incomplete data for ticker {ticker}: cik={cik}, name={name}")
                        failed_tickers.append(ticker)
                else:
                    logger.debug(f"Failed to lookup ticker {ticker}: {result.get('error', 'Unknown error')}")
                    failed_tickers.append(ticker)
            else:
                logger.debug(f"No result for ticker {ticker}")
                failed_tickers.append(ticker)
        
        logger.info(f"Successfully looked up {len(results)} tickers, {len(failed_tickers)} failed")
        
    except Exception as e:
        logger.error(f"Error during batch CIK lookup: {e}")
        raise RuntimeError(f"Failed to lookup CIK and company names: {e}")
    
    return results, failed_tickers


def process_tickers_in_batches(tickers: List[str], batch_size: int = 100) -> Tuple[Dict[str, Tuple[int, str]], List[str]]:
    """
    Process tickers in batches to avoid overwhelming the API.
    
    Args:
        tickers: List of ticker symbols to process
        batch_size: Number of tickers to process per batch
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to (cik, company_name) tuples
        - List of all tickers that failed lookup
    """
    all_results = {}
    all_failed = []
    
    total_batches = (len(tickers) + batch_size - 1) // batch_size
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)...")
        
        batch_results, batch_failed = lookup_cik_and_company_name_batch(batch)
        
        all_results.update(batch_results)
        all_failed.extend(batch_failed)
    
    return all_results, all_failed


def create_cik_lookup_entities(ticker_cik_map: Dict[str, Tuple[int, str]]) -> Dict[int, CikLookup]:
    """
    Create CikLookup entities from ticker to CIK/company name mapping.
    Groups by CIK since multiple tickers can map to the same CIK.
    
    Args:
        ticker_cik_map: Dictionary mapping ticker to (cik, company_name) tuples
        
    Returns:
        Dictionary mapping CIK to CikLookup entity
    """
    cik_entities = {}
    
    for ticker, (cik, company_name) in ticker_cik_map.items():
        if cik not in cik_entities:
            # Create new CikLookup entity
            try:
                cik_lookup = CikLookup(cik=cik, company_name=company_name)
                cik_entities[cik] = cik_lookup
            except Exception as e:
                logger.warning(f"Failed to create CikLookup for CIK {cik} ({company_name}): {e}")
        else:
            # CIK already exists, verify company name matches
            existing = cik_entities[cik]
            if existing.company_name != company_name:
                logger.debug(f"CIK {cik} has multiple company names: '{existing.company_name}' vs '{company_name}'")
    
    logger.info(f"Created {len(cik_entities)} unique CikLookup entities from {len(ticker_cik_map)} tickers")
    
    return cik_entities


def analyze_synchronization_operations(
    database_ciks: Dict[int, CikLookup],
    source_ciks: Dict[int, CikLookup]
) -> SynchronizationResult:
    """
    Analyze differences between database and source CIK data.
    
    Args:
        database_ciks: Dictionary of CIK to CikLookup entities currently in database
        source_ciks: Dictionary of CIK to CikLookup entities from source data
        
    Returns:
        SynchronizationResult containing operations to perform
    """
    sync_result = SynchronizationResult()
    
    # Find CIKs to add (in source but not in database)
    for cik, cik_lookup in source_ciks.items():
        if cik not in database_ciks:
            sync_result.to_add.append(cik_lookup)
    
    # Find CIKs to delete (in database but not in source)
    for cik in database_ciks.keys():
        if cik not in source_ciks:
            sync_result.to_delete.append(cik)
    
    # Find CIKs to update (company name changed) or unchanged
    for cik, source_lookup in source_ciks.items():
        if cik in database_ciks:
            db_lookup = database_ciks[cik]
            if db_lookup.company_name != source_lookup.company_name:
                # Keep the database timestamps, just update the company name
                source_lookup.created_at = db_lookup.created_at
                source_lookup.last_updated_at = db_lookup.last_updated_at
                sync_result.to_update.append(source_lookup)
            else:
                # Unchanged CIK
                sync_result.unchanged.append(cik)
    
    stats = sync_result.get_stats()
    logger.info(f"Analysis: {stats['to_add']} to add, {stats['to_delete']} to delete, "
                f"{stats['to_update']} to update, {stats['unchanged']} unchanged")
    
    return sync_result
