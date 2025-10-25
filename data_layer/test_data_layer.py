"""
Simple test suite for the Stock Data Layer.

Run this to verify that the data layer is working correctly.
"""

import unittest
import os
import sys
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer import (
    DatabaseConnectionManager,
    StocksRepository,
    Stock,
    StockNotFoundError,
    DuplicateStockError,
    ValidationError
)


class TestStockModel(unittest.TestCase):
    """Test cases for the Stock model."""
    
    def test_valid_stock_creation(self):
        """Test creating a valid stock."""
        stock = Stock(
            symbol="AAPL",
            company="Apple Inc.",
            exchange="NASDAQ"
        )
        
        self.assertEqual(stock.symbol, "AAPL")
        self.assertEqual(stock.company, "Apple Inc.")
        self.assertEqual(stock.exchange, "NASDAQ")
    
    def test_symbol_validation(self):
        """Test symbol validation."""
        # Empty symbol should fail
        with self.assertRaises(ValidationError):
            Stock(symbol="")
        
        # Long symbol should fail
        with self.assertRaises(ValidationError):
            Stock(symbol="A" * 25)
        
        # Invalid characters should fail
        with self.assertRaises(ValidationError):
            Stock(symbol="AA@PL")
    
    def test_symbol_normalization(self):
        """Test that symbols are normalized to uppercase."""
        stock = Stock(symbol="aapl")
        self.assertEqual(stock.symbol, "AAPL")
    
    def test_to_dict_conversion(self):
        """Test converting stock to dictionary."""
        stock = Stock(
            symbol="AAPL",
            company="Apple Inc.",
            exchange="NASDAQ"
        )
        
        data = stock.to_dict(include_timestamps=False)
        
        self.assertEqual(data['symbol'], "AAPL")
        self.assertEqual(data['company'], "Apple Inc.")
        self.assertEqual(data['exchange'], "NASDAQ")
        # ID is optional now since symbol is the primary key
        self.assertIn('id', data)  # Still included in dict for compatibility
    
    def test_from_dict_creation(self):
        """Test creating stock from dictionary."""
        data = {
            'symbol': 'AAPL',
            'company': 'Apple Inc.',
            'exchange': 'NASDAQ'
        }
        
        stock = Stock.from_dict(data)
        
        self.assertEqual(stock.symbol, 'AAPL')
        self.assertEqual(stock.company, 'Apple Inc.')
        self.assertEqual(stock.exchange, 'NASDAQ')


class TestDatabaseConnectionManager(unittest.TestCase):
    """Test cases for the DatabaseConnectionManager."""
    
    def setUp(self):
        """Set up test environment."""
        if not os.getenv('DATABASE_URL'):
            self.skipTest("DATABASE_URL environment variable not set")
    
    def test_connection_creation(self):
        """Test creating a database connection."""
        db_manager = DatabaseConnectionManager()
        self.assertIsNotNone(db_manager)
    
    def test_connection_test(self):
        """Test database connection."""
        db_manager = DatabaseConnectionManager()
        result = db_manager.test_connection()
        self.assertTrue(result, "Database connection should be successful")
    
    def test_context_manager(self):
        """Test connection context manager."""
        db_manager = DatabaseConnectionManager()
        
        with db_manager.get_connection_context() as conn:
            self.assertIsNotNone(conn)
            
        with db_manager.get_cursor_context() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)


class TestStockRepository(unittest.TestCase):
    """Test cases for the StockRepository."""
    
    def setUp(self):
        """Set up test environment."""
        if not os.getenv('DATABASE_URL'):
            self.skipTest("DATABASE_URL environment variable not set")
        
        self.db_manager = DatabaseConnectionManager()
        self.repo = StocksRepository(self.db_manager)
        
        # Clean up any existing test data
        self.cleanup_test_data()
    
    def tearDown(self):
        """Clean up after tests."""
        self.cleanup_test_data()
        if hasattr(self, 'db_manager'):
            self.db_manager.close_all_connections()
    
    def cleanup_test_data(self):
        """Remove any test stocks from the database."""
        test_symbols = ["TEST", "TESTCRUD", "BULK1", "BULK2", "BULK3"]
        for symbol in test_symbols:
            try:
                self.repo.delete_by_symbol(symbol)
            except:
                pass
    
    def test_repository_creation(self):
        """Test creating a stock repository."""
        self.assertIsNotNone(self.repo)
    
    def test_count_stocks(self):
        """Test counting stocks."""
        count = self.repo.count()
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)
    
    def test_create_and_retrieve_stock(self):
        """Test creating and retrieving a stock."""
        # Create a test stock
        stock = Stock(
            symbol="TESTCRUD",
            company="Test CRUD Company",
            exchange="TEST"
        )
        
        created_stock = self.repo.create(stock)
        
        # Verify the stock was created with timestamps
        self.assertIsNotNone(created_stock.created_at)
        self.assertIsNotNone(created_stock.last_updated_at)
        
        # Retrieve by symbol (primary key)
        retrieved_by_symbol = self.repo.get_by_symbol("TESTCRUD")
        self.assertIsNotNone(retrieved_by_symbol)
        self.assertEqual(retrieved_by_symbol.symbol, "TESTCRUD")
    
    def test_duplicate_stock_error(self):
        """Test that creating duplicate stock raises error."""
        stock1 = Stock(symbol="TEST", company="Test 1")
        stock2 = Stock(symbol="TEST", company="Test 2")
        
        self.repo.create(stock1)
        
        with self.assertRaises(DuplicateStockError):
            self.repo.create(stock2)
    
    def test_update_stock(self):
        """Test updating a stock."""
        # Create a stock
        stock = Stock(
            symbol="TEST",
            company="Original Company",
            exchange="NYSE"
        )
        created_stock = self.repo.create(stock)
        
        # Update the stock
        created_stock.company = "Updated Company"
        created_stock.exchange = "NASDAQ"
        
        updated_stock = self.repo.update(created_stock)
        
        # Verify updates
        self.assertEqual(updated_stock.company, "Updated Company")
        self.assertEqual(updated_stock.exchange, "NASDAQ")
        
        # Verify in database
        retrieved = self.repo.get_by_symbol(created_stock.symbol)
        self.assertEqual(retrieved.company, "Updated Company")
        self.assertEqual(retrieved.exchange, "NASDAQ")
    
    def test_delete_stock(self):
        """Test deleting a stock."""
        # Create a stock
        stock = Stock(symbol="TEST", company="Test Company")
        created_stock = self.repo.create(stock)
        
        # Verify it exists
        self.assertIsNotNone(self.repo.get_by_symbol(created_stock.symbol))
        
        # Delete it
        success = self.repo.delete(created_stock.symbol)
        self.assertTrue(success)
        
        # Verify it's gone
        self.assertIsNone(self.repo.get_by_symbol(created_stock.symbol))
    
    def test_search_functionality(self):
        """Test stock search functionality."""
        # Create some test stocks
        test_stocks = [
            Stock(symbol="BULK1", company="Bulk Company 1", exchange="NYSE"),
            Stock(symbol="BULK2", company="Bulk Company 2", exchange="NASDAQ"),
            Stock(symbol="TEST", company="Different Company", exchange="NYSE")
        ]
        
        for stock in test_stocks:
            self.repo.create(stock)
        
        # Test search by symbol pattern
        bulk_results = self.repo.search(symbol_pattern="BULK")
        self.assertEqual(len(bulk_results), 2)
        
        # Test search by exchange
        nyse_results = self.repo.search(exchange="NYSE")
        self.assertGreaterEqual(len(nyse_results), 2)
        
        # Test search by company pattern
        bulk_company_results = self.repo.search(company_pattern="Bulk Company")
        self.assertEqual(len(bulk_company_results), 2)
    
    def test_bulk_insert(self):
        """Test bulk insert functionality."""
        stocks = [
            Stock(symbol="BULK1", company="Bulk 1"),
            Stock(symbol="BULK2", company="Bulk 2"),
            Stock(symbol="BULK3", company="Bulk 3")
        ]
        
        created_stocks = self.repo.bulk_insert(stocks)
        self.assertEqual(len(created_stocks), 3)
        
        # Verify all stocks were created
        for stock in created_stocks:
            retrieved = self.repo.get_by_symbol(stock.symbol)
            self.assertIsNotNone(retrieved)


def run_tests():
    """Run all tests."""
    print("=== Running Stock Data Layer Tests ===\n")
    
    # Check if database is available
    if not os.getenv('DATABASE_URL'):
        print("⚠ DATABASE_URL environment variable not set. Skipping database tests.")
        print("⚠ Only running model validation tests.")
        
        # Run only model tests
        suite = unittest.TestLoader().loadTestsFromTestCase(TestStockModel)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        return result.wasSuccessful()
    
    # Run all tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestStockModel))
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseConnectionManager))
    suite.addTests(loader.loadTestsFromTestCase(TestStockRepository))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print(f"\n=== Test Results ===")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)