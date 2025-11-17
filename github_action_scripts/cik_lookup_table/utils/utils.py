"""
Utility functions for CIK lookup table synchronization.
"""

import html
import logging
import os
import re
import sys
import unicodedata
from typing import Dict, List, Tuple, Set

# Add data layer to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from data_layer.models import CikLookup
from data_layer.repositories import CikLookupRepository, TickerSummaryRepository

# Add entities and constants to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from entities.synchronization_result import SynchronizationResult
from constants import BATCH_SIZE

logger = logging.getLogger(__name__)


# ============================================================================
# Company Name Search Normalization
# ============================================================================

# Legal entity suffixes to remove
LEGAL_SUFFIXES = [
    # Common English forms
    'incorporated', 'corporation', 'company', 'limited',
    # Abbreviated forms with and without periods
    'inc.', 'inc', 'corp.', 'corp', 'co.', 'co', 'ltd.', 'ltd',
    'llc', 'l.l.c.', 'l.l.c', 'lp', 'l.p.', 'l.p', 'llp', 'l.l.p.', 'l.l.p',
    'plc',
    # International forms
    'gmbh', 'ag', 's.a.', 's.a', 'sa', 'n.v.', 'n.v', 'nv', 'b.v.', 'b.v', 'bv',
    'spa', 's.p.a.', 's.p.a', 'sarl', 's.a.r.l.', 's.a.r.l',
    'pty.', 'pty', 'srl', 's.r.l.', 's.r.l',
    # Nordic/other
    'a/s', 'as', 'oy', 'ab', 'oyj',
]

# Ownership, structure, and finance words
STRUCTURE_WORDS = [
    'holdings', 'holding', 'hldgs', 'hldg',
    'group', 'group.',
    'partners', 'partner',
    'trust', 'trustee', 'trustees',
    'fund', 'funds', 'investment', 'investments', 'capital', 'ventures', 'venture',
    'management', 'mgmt', 'advisors', 'advisor', 'advisory',
    'securities', 'assets',
    'realty', 'properties', 'property',
    'bancorp', 'financial', 'finance',
]

# Transaction and lifecycle words
TRANSACTION_WORDS = [
    'acquisition', 'acquisitions', 'acquire', 'acquired', 'acq.', 'acq',
    'merger', 'mergers', 'merged', 'mrg',
    'purchase', 'purchases', 'bought', 'buying',
    'takeover', 'spin-off', 'spinoff',
    'divestiture', 'divest', 'divests',
    'reorganization', 'reorg', 'restructuring',
]

# Industry and business descriptors
INDUSTRY_WORDS = [
    'technology', 'technologies', 'tech', 'systems', 'solutions', 'software',
    'services', 'service', 'networks', 'network', 'telecom', 'telecommunications', 'communications',
    'international', 'intl', 'intl.', 'global', 'globals', 'worldwide',
    'industrial', 'industries', 'industrials',
    'manufacturing', 'manuf', 'manuf.',
    'engineering', 'engineering.',
    'digital', 'media', 'energy', 'resources',
    'healthcare', 'health', 'medical', 'bio', 'biotech', 'biosciences', 'bioscience',
    'pharmaceutical', 'pharma', 'pharmaceuticals',
    'retail', 'consumer', 'commercial',
]

# Common stopwords and connectors
STOPWORDS = [
    'the', 'of', 'for', 'by', 'in'
]

# Abbreviations
ABBREVIATIONS = [
    'svc', 'svc.', 'svcs', 'svcs.',
    'sys', 'sys.',
    'mfg', 'mfg.',
    'trx',
    'biz',
]

# Financial instruments
FINANCIAL_TERMS = [
    'etf', 'etn', 'reit', 'spv', 'spac',
]

# Build comprehensive removal list (sorted by descending length for proper matching)
REMOVAL_WORDS = sorted(
    set(LEGAL_SUFFIXES + STRUCTURE_WORDS + TRANSACTION_WORDS + 
        INDUSTRY_WORDS + STOPWORDS + ABBREVIATIONS + FINANCIAL_TERMS),
    key=len,
    reverse=True
)


def normalize_company_name_for_search(company_name: str) -> str:
    """
    Normalize company name for search by removing legal suffixes, common words,
    punctuation, and applying Unicode normalization. This produces a clean,
    lowercase, space-free string optimized for similarity matching.
    
    Normalization steps (in order):
    1. Unicode normalize (NFKC) and strip BOM
    2. Convert to lowercase
    3. Decode HTML entities (&amp; etc.)
    4. Normalize diacritics to ASCII (remove accents)
    5. Replace ampersand with space
    6. Remove all punctuation and symbols (keep only letters, digits, spaces)
    7. Remove common legal/business words as whole words
    8. Remove all remaining whitespace
    
    Args:
        company_name: Original company name
        
    Returns:
        Normalized search string (lowercase, no spaces, no punctuation)
        
    Examples:
        "Acme Holdings, Inc." -> "acme"
        "The ABC Co., Ltd." -> "abc"
        "Global-Tech Systems, LLC" -> "globaltech"
        "Johnson & Johnson" -> "johnsonjohnson"
        "Société Générale S.A." -> "societe"
    """
    if not company_name:
        return ""
    
    # Step 1: Unicode normalize (NFKC handles compatibility characters)
    normalized = unicodedata.normalize('NFKC', company_name)
    
    # Step 2: Convert to lowercase
    normalized = normalized.lower()
    
    # Step 3: Decode HTML entities (e.g., &amp; -> &)
    normalized = html.unescape(normalized)
    
    # Step 4: Normalize diacritics to ASCII
    # NFKD decomposes characters, then filter out combining marks
    nfkd_form = unicodedata.normalize('NFKD', normalized)
    ascii_text = ''.join(char for char in nfkd_form if not unicodedata.combining(char))
    normalized = ascii_text
    
    # Step 5: Replace ampersand with space (before removing punctuation)
    normalized = normalized.replace('&', ' ')
    
    # Step 6: Remove punctuation and symbols - keep only letters, digits, and spaces
    # This removes: . , : ; ' " ` ! ? ( ) [ ] { } - _ / \ | + = * ^ % $ # @ ~ < >
    normalized = re.sub(r'[^a-z0-9\s]', '', normalized)
    
    # Step 7: Remove common words as whole words
    # Build regex pattern with word boundaries
    if REMOVAL_WORDS:
        # Escape special regex characters in words
        escaped_words = [re.escape(word) for word in REMOVAL_WORDS]
        pattern = r'\b(' + '|'.join(escaped_words) + r')\b'
        normalized = re.sub(pattern, ' ', normalized)
    
    # Step 8: Collapse whitespace and remove all spaces
    normalized = re.sub(r'\s+', '', normalized)
    
    # Final cleanup: strip any remaining whitespace (shouldn't be any)
    normalized = normalized.strip()
    
    # Handle edge case: if normalization removed everything, return empty string
    # (caller can decide whether to use original or handle specially)
    return normalized


def normalize_company_name(name: str) -> str:
    """
    Normalize company name by:
    1. Removing commas before corporate suffixes
    2. Standardizing suffix formats to include periods
    
    Args:
        name: Original company name
        
    Returns:
        Normalized company name
    """
    suffix_patterns = [
        # With comma patterns (need to remove comma)
        (r',\s*INC\.?', ' Inc.'),
        (r',\s*INCORPORATED', ' Inc.'),
        (r',\s*CORP\.?', ' Corp.'),
        (r',\s*CORPORATION', ' Corp.'),
        (r',\s*LLC\.?', ' LLC'),
        (r',\s*L\.L\.C\.?', ' LLC'),
        (r',\s*LTD\.?', ' Ltd.'),
        (r',\s*LIMITED', ' Ltd.'),
        (r',\s*L\.P\.?', ' L.P.'),
        (r',\s*LP\.?', ' L.P.'),
        (r',\s*CO\.?', ' Co.'),
        (r',\s*COMPANY', ' Co.'),
        (r',\s*PLC\.?', ' PLC'),
        (r',\s*N\.V\.?', ' N.V.'),
        (r',\s*S\.A\.?', ' S.A.'),
        (r',\s*AG\.?', ' AG'),
        (r',\s*GMBH\.?', ' GmbH'),
        
        # Without comma patterns (just standardize format)
        (r'\bINC\b(?!\.)', ' Inc.'),
        (r'\bINCORPORATED\b', ' Inc.'),
        (r'\bCORP\b(?!\.)', ' Corp.'),
        (r'\bCORPORATION\b', ' Corp.'),
        (r'\bLLC\b(?!\.)', ' LLC'),
        (r'\bL\.L\.C\b(?!\.)', ' LLC'),
        (r'\bLTD\b(?!\.)', ' Ltd.'),
        (r'\bLIMITED\b', ' Ltd.'),
        (r'\bL\.P\b(?!\.)', ' L.P.'),
        (r'\bLP\b(?!\.)', ' L.P.'),
        (r'\bCO\b(?!\.)', ' Co.'),
        (r'\bCOMPANY\b', ' Co.'),
        (r'\bPLC\b(?!\.)', ' PLC'),
        (r'\bN\.V\b(?!\.)', ' N.V.'),
        (r'\bS\.A\b(?!\.)', ' S.A.'),
        (r'\bAG\b(?!\.)', ' AG'),
        (r'\bGMBH\b(?!\.)', ' GmbH'),
    ]
    
    normalized = name
    for pattern, replacement in suffix_patterns:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()


def clean_company_name(name: str) -> str:
    """
    Clean company name by removing parenthetical information and trailing slashes.
    
    Removes:
    1. Parentheses with content (regions, jurisdictions, stock exchanges, etc.)
    2. Forward slashes with trailing content (jurisdiction codes)
    
    Preserves:
    - Slashes that are part of company names (A/S, SA/NV, I/O, Long/Short, etc.)
    
    Args:
        name: Original company name
        
    Returns:
        Cleaned company name
    """
    # Remove parentheses with any content inside
    name = re.sub(r'\s*\([^)]*\)', '', name)
    
    # Identify company type designations that use slashes and should be kept
    company_types = ['A/S', 'SA/NV', 'I/O', 'Long/Short']
    has_company_type_at_end = any(name.endswith(ct) for ct in company_types)
    
    # Remove space + slash + jurisdiction code patterns
    name = re.sub(r'\s+/\s*[A-Z]{2,}/?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+/\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$', '', name)
    
    # Remove no-space slash + jurisdiction (if not a company type)
    if not has_company_type_at_end:
        name = re.sub(r'/[A-Z]{2,}(?:\s+[A-Z][a-z]+)*/?$', '', name)
        name = re.sub(r'/[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$', '', name)
        name = re.sub(r'/[A-Za-z]{2,}$', '', name)
    
    # Remove trailing slash
    if name.endswith('/') and not any(ct in name for ct in company_types):
        name = name.rstrip('/')
    
    # Clean up whitespace
    return ' '.join(name.split())


def process_company_name(name: str) -> str:
    """
    Apply both normalization and cleaning in sequence.
    Order matters: normalize first, then clean.
    
    Args:
        name: Original company name
        
    Returns:
        Processed company name
    """
    normalized = normalize_company_name(name)
    cleaned = clean_company_name(normalized)
    return cleaned


# ============================================================================
# CIK Lookup Specific Functions
# ============================================================================
# Note: fetch_ticker_data_from_github_repo and lookup_cik_batch are now imported from common utils


def lookup_cik_and_company_name_batch(tickers: List[str]) -> Tuple[Dict[str, Tuple[int, str]], List[str]]:
    """
    Lookup CIK and company name for multiple tickers using sec-company-lookup.
    This is specific to CIK lookup table - extends the common lookup_cik_batch to also get company names.
    
    Args:
        tickers: List of ticker symbols to lookup
        
    Returns:
        Tuple of:
        - Dictionary mapping ticker to (cik, company_name) tuples
        - List of tickers that failed lookup
    """
    from sec_company_lookup import get_companies_by_tickers
    
    results: Dict[str, Tuple[int, str]] = {}
    failed_tickers: List[str] = []
    
    try:
        # Use batch lookup for efficiency
        logger.info(f"Looking up CIK and company names for {len(tickers)} tickers...")
        batch_results = get_companies_by_tickers(tickers)
        
        if batch_results is None:
            logger.error("Batch lookup returned None")
            raise RuntimeError("Failed to lookup CIK and company names: batch lookup returned None")
        
        for ticker in tickers:
            if ticker in batch_results:  # type: ignore
                result = batch_results[ticker]  # type: ignore
                
                if result.get('success') and result.get('data'):  # type: ignore
                    company_data = result['data']  # type: ignore
                    cik = company_data.get('cik')  # type: ignore
                    name = company_data.get('name')  # type: ignore
                    
                    if cik is not None and name:
                        # Apply normalization and cleaning to company name
                        # Cast/convert name to string. Ignore static type-checkers here.
                        name_str = str(name)  # type: ignore
                        processed_name = process_company_name(name_str)
                        results[ticker] = (cik, processed_name)

                        # Log if name was modified
                        if processed_name != name_str:
                            logger.debug(f"Processed company name for {ticker}: '{name_str}' -> '{processed_name}'")
                    else:
                        logger.debug(f"Incomplete data for ticker {ticker}: cik={cik}, name={name}")
                        failed_tickers.append(ticker)
                else:
                    logger.debug(f"Failed to lookup ticker {ticker}: {result.get('error', 'Unknown error')}")  # type: ignore
                    failed_tickers.append(ticker)
            else:
                logger.debug(f"No result for ticker {ticker}")
                failed_tickers.append(ticker)
        
        logger.info(f"Successfully looked up {len(results)} tickers, {len(failed_tickers)} failed")
        
    except Exception as e:
        logger.error(f"Error during batch CIK lookup: {e}")
        raise RuntimeError(f"Failed to lookup CIK and company names: {e}")
    
    return results, failed_tickers


def process_tickers_and_persist_ciks(
    tickers: List[str],
    cik_repo: CikLookupRepository,
    database_ciks: Dict[int, CikLookup]
) -> SynchronizationResult:
    """
    Process tickers in batches, lookup CIKs, and immediately persist to database.
    This ensures data is saved incrementally as it's retrieved, not all at once.
    
    Args:
        tickers: List of ticker symbols to process
        cik_repo: CIK lookup repository for database operations
        database_ciks: Dictionary of existing CIKs in database for comparison
        
    Returns:
        SynchronizationResult containing operation statistics
    """
    sync_result = SynchronizationResult()
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Processing {len(tickers)} tickers in {total_batches} batches of {BATCH_SIZE}")
    
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)...")
        
        # Lookup CIKs for this batch
        batch_results, batch_failed = lookup_cik_and_company_name_batch(batch)
        
        # Track failed lookups
        sync_result.failed_ticker_lookups.extend(batch_failed)
        
        # Group results by CIK (multiple tickers can map to same CIK)
        cik_to_company_name: Dict[int, str] = {}
        for _, (cik, company_name) in batch_results.items():
            if cik not in cik_to_company_name:
                cik_to_company_name[cik] = company_name
            elif cik_to_company_name[cik] != company_name:
                logger.debug(f"CIK {cik} has multiple company names: '{cik_to_company_name[cik]}' vs '{company_name}'")
        
        # Categorize CIKs and persist immediately
        ciks_to_add: List[CikLookup] = []
        ciks_to_update: List[CikLookup] = []
        
        for cik, company_name in cik_to_company_name.items():
            # Compute normalized search string for this company name
            company_name_search = normalize_company_name_for_search(company_name)
            
            if cik in database_ciks:
                existing = database_ciks[cik]
                if existing.company_name != company_name or existing.company_name_search != company_name_search:
                    updated_cik = CikLookup(
                        cik=cik,
                        company_name=company_name,
                        company_name_search=company_name_search,
                        created_at=existing.created_at,
                        last_updated_at=existing.last_updated_at
                    )
                    ciks_to_update.append(updated_cik)
                else:
                    # Unchanged - track it
                    sync_result.unchanged.append(cik)
            else:
                # New CIK - add it
                new_cik = CikLookup(
                    cik=cik,
                    company_name=company_name,
                    company_name_search=company_name_search
                )
                ciks_to_add.append(new_cik)
        
        # Immediately persist new CIKs to database
        if ciks_to_add:
            try:
                added_count = cik_repo.bulk_insert(ciks_to_add)
                logger.info(f"Batch {batch_num}: Added {added_count} new CIKs to database")
                sync_result.to_add.extend(ciks_to_add)
                # Update local cache so subsequent batches see these as existing
                for cik_lookup in ciks_to_add:
                    database_ciks[cik_lookup.cik] = cik_lookup
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to add CIKs: {e}")
                raise
        
        # Immediately persist updated CIKs to database
        if ciks_to_update:
            try:
                updated_count = cik_repo.bulk_update(ciks_to_update)
                logger.info(f"Batch {batch_num}: Updated {updated_count} CIKs in database")
                sync_result.to_update.extend(ciks_to_update)
                # Update local cache with new company names
                for cik_lookup in ciks_to_update:
                    database_ciks[cik_lookup.cik] = cik_lookup
            except Exception as e:
                logger.error(f"Batch {batch_num}: Failed to update CIKs: {e}")
                raise
    
    logger.info(f"Completed processing all {total_batches} batches")
    logger.info(f"Total: {len(sync_result.to_add)} added, {len(sync_result.to_update)} updated, "
                f"{len(sync_result.unchanged)} unchanged, {len(sync_result.failed_ticker_lookups)} failed lookups")
    
    return sync_result


def identify_ciks_to_delete(
    database_ciks: Dict[int, CikLookup],
    processed_ciks: Set[int]
) -> List[int]:
    """
    Identify CIKs in database that were not found in the source data.
    These should be deleted as they are no longer valid.
    
    Args:
        database_ciks: Dictionary of all CIKs currently in database
        processed_ciks: Set of CIK numbers that were found in source data
        
    Returns:
        List of CIK numbers to delete from database
    """
    ciks_to_delete: List[int] = []
    
    for cik in database_ciks.keys():
        if cik not in processed_ciks:
            ciks_to_delete.append(cik)
    
    if ciks_to_delete:
        logger.info(f"Found {len(ciks_to_delete)} CIKs in database that are not in source data")
    
    return ciks_to_delete


def delete_obsolete_ciks(
    cik_repo: CikLookupRepository,
    ticker_summary_repo: TickerSummaryRepository,
    ciks_to_delete: List[int]
) -> int:
    """
    Delete CIKs from database that are no longer in source data.
    First deletes from ticker_summary table to avoid foreign key constraint violations,
    then deletes from cik_lookup table.
    
    Args:
        cik_repo: CIK lookup repository for database operations
        ticker_summary_repo: Ticker summary repository for database operations
        ciks_to_delete: List of CIK numbers to delete
        
    Returns:
        Number of CIKs successfully deleted
    """
    if not ciks_to_delete:
        logger.info("No obsolete CIKs to delete")
        return 0
    
    logger.info(f"Deleting {len(ciks_to_delete)} obsolete CIKs in batches of {BATCH_SIZE}")
    
    total_deleted = 0
    total_batches = (len(ciks_to_delete) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for i in range(0, len(ciks_to_delete), BATCH_SIZE):
        batch = ciks_to_delete[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        try:
            # First delete from ticker_summary table to avoid foreign key constraint
            ticker_summary_deleted = ticker_summary_repo.bulk_delete_by_cik(batch)
            logger.info(f"Delete batch {batch_num}/{total_batches}: Deleted {ticker_summary_deleted} ticker summaries")
            
            # Then delete from cik_lookup table
            deleted_count = cik_repo.bulk_delete(batch)
            total_deleted += deleted_count
            logger.info(f"Delete batch {batch_num}/{total_batches}: Deleted {deleted_count}/{len(batch)} CIKs")
        except Exception as e:
            logger.error(f"Delete batch {batch_num}: Failed to delete CIKs: {e}")
            raise
    
    logger.info(f"Successfully deleted {total_deleted} obsolete CIKs from database")
    return total_deleted
