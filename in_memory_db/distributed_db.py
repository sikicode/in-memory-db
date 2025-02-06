import threading
import redis
import psycopg2
from psycopg2.extras import DictCursor
from loguru import logger
from prometheus_client import Counter, Gauge, start_http_server
from flask import Flask, jsonify
import os

app = Flask(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter('db_request_total', 'Total number of requests')
ACTIVE_TRANSACTIONS = Gauge('db_active_transactions', 'Number of active transactions')

class DistributedDB:
    def __init__(self):
        # Redis for distributed locking
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )
        
        # PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            dbname=os.getenv('PG_DB', 'postgres'),
            user=os.getenv('PG_USER', 'postgres'),
            password=os.getenv('PG_PASSWORD', 'postgres'),
            host=os.getenv('PG_HOST', 'localhost'),
            port=int(os.getenv('PG_PORT', 5432)
        )
        
        # Initialize database tables
        self._init_db()
        
        # Local thread lock
        self._lock = threading.Lock()
        
    def _init_db(self):
        with self.pg_conn.cursor() as cur:
            # Create tables if they don't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS key_value_store (
                    name VARCHAR PRIMARY KEY,
                    value VARCHAR
                );
                CREATE TABLE IF NOT EXISTS value_count (
                    value VARCHAR PRIMARY KEY,
                    count INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    status VARCHAR
                );
            """)
            self.pg_conn.commit()
    
    def _acquire_lock(self, key, timeout=10):
        return self.redis_client.set(f'lock:{key}', 1, nx=True, ex=timeout)
    
    def _release_lock(self, key):
        self.redis_client.delete(f'lock:{key}')
    
    def set(self, name, value):
        REQUEST_COUNT.inc()
        if not self._acquire_lock(name):
            logger.error(f"Could not acquire lock for key: {name}")
            return False
            
        try:
            with self.pg_conn.cursor() as cur:
                # Get old value
                cur.execute("SELECT value FROM key_value_store WHERE name = %s", (name,))
                old_value = cur.fetchone()
                
                # Update value counts
                if old_value:
                    cur.execute("UPDATE value_count SET count = count - 1 WHERE value = %s", (old_value[0],))
                cur.execute("""
                    INSERT INTO value_count (value, count) 
                    VALUES (%s, 1) 
                    ON CONFLICT (value) DO UPDATE SET count = value_count.count + 1
                """, (value,))
                
                # Update key-value store
                cur.execute("""
                    INSERT INTO key_value_store (name, value) 
                    VALUES (%s, %s) 
                    ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value
                """, (name, value))
                
                self.pg_conn.commit()
                logger.info(f"Successfully set {name}={value}")
                return True
        finally:
            self._release_lock(name)
    
    def get(self, name):
        REQUEST_COUNT.inc()
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT value FROM key_value_store WHERE name = %s", (name,))
            result = cur.fetchone()
            return result[0] if result else 'NULL'
    
    def numequalto(self, value):
        REQUEST_COUNT.inc()
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT count FROM value_count WHERE value = %s", (value,))
            result = cur.fetchone()
            return result[0] if result else 0

# Health check endpoint
@app.route('/health')
def health_check():
    return jsonify({'status': 'healthy'})

def main():
    # Start Prometheus metrics server
    start_http_server(8000)
    
    # Start Flask server
    app.run(host='0.0.0.0', port=5000)
    
    db = DistributedDB()
    
    while True:
        try:
            command = input().strip().split()
            if not command:
                continue

            cmd = command[0].upper()

            if cmd == 'END':
                break
            elif cmd == 'SET' and len(command) == 3:
                db.set(command[1], command[2])
            elif cmd == 'GET' and len(command) == 2:
                print(db.get(command[1]))
            elif cmd == 'NUMEQUALTO' and len(command) == 2:
                print(db.numequalto(command[2]))
            else:
                print('Invalid command')

        except EOFError:
            break
        except Exception as e:
            logger.error(f"Error processing command: {e}")

if __name__ == '__main__':
    main()