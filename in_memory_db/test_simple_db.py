import unittest
import threading
from simple_db import InMemoryDB

class TestInMemoryDB(unittest.TestCase):
    def setUp(self):
        self.db = InMemoryDB()
    
    def test_basic_operations(self):
        # Test SET and GET
        self.db.set('x', '10')
        self.assertEqual(self.db.get('x'), '10')
        
        # Test non-existent key
        self.assertEqual(self.db.get('y'), 'NULL')
        
        # Test UNSET
        self.db.unset('x')
        self.assertEqual(self.db.get('x'), 'NULL')
    
    def test_numequalto(self):
        # Test with no values
        self.assertEqual(self.db.numequalto('10'), 0)
        
        # Test with one value
        self.db.set('x', '10')
        self.assertEqual(self.db.numequalto('10'), 1)
        
        # Test with multiple same values
        self.db.set('y', '10')
        self.assertEqual(self.db.numequalto('10'), 2)
        
        # Test after unset
        self.db.unset('y')
        self.assertEqual(self.db.numequalto('10'), 1)
    
    def test_simple_transaction(self):
        # Test basic transaction
        self.db.set('x', '10')
        self.db.begin()
        self.db.set('x', '20')
        self.assertEqual(self.db.get('x'), '20')
        self.db.rollback()
        self.assertEqual(self.db.get('x'), '10')
    
    def test_nested_transactions(self):
        # Test nested transactions
        self.db.set('x', '10')
        
        self.db.begin()
        self.db.set('x', '20')
        
        self.db.begin()
        self.db.set('x', '30')
        self.assertEqual(self.db.get('x'), '30')
        
        self.db.rollback()
        self.assertEqual(self.db.get('x'), '20')
        
        self.db.rollback()
        self.assertEqual(self.db.get('x'), '10')
    
    def test_commit(self):
        # Test commit in nested transactions
        self.db.set('x', '10')
        
        self.db.begin()
        self.db.set('x', '20')
        
        self.db.begin()
        self.db.set('x', '30')
        
        self.db.commit()
        self.assertEqual(self.db.get('x'), '30')
        
        # Rollback should have no effect after commit
        self.assertEqual(self.db.rollback(), 'NO TRANSACTION')
        self.assertEqual(self.db.get('x'), '30')
    
    def test_no_transaction(self):
        # Test rollback with no transaction
        self.assertEqual(self.db.rollback(), 'NO TRANSACTION')
        
        # Test commit with no transaction
        self.assertEqual(self.db.commit(), 'NO TRANSACTION')
    
    def test_value_counting_in_transaction(self):
        # Test value counting within transactions
        self.db.set('x', '10')
        self.db.set('y', '10')
        self.assertEqual(self.db.numequalto('10'), 2)
        
        self.db.begin()
        self.db.set('x', '20')
        self.assertEqual(self.db.numequalto('10'), 1)
        self.assertEqual(self.db.numequalto('20'), 1)
        
        self.db.rollback()
        self.assertEqual(self.db.numequalto('10'), 2)
        self.assertEqual(self.db.numequalto('20'), 0)
    
    def test_concurrent_access(self):
        def worker():
            for i in range(100):
                self.db.set(f'key{i}', str(i))
                self.assertEqual(self.db.get(f'key{i}'), str(i))
        
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify all values are correctly set
        for i in range(100):
            self.assertEqual(self.db.get(f'key{i}'), str(i))

if __name__ == '__main__':
    unittest.main()