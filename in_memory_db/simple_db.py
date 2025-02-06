import threading

class InMemoryDB:
    def __init__(self):
        # Main database storage
        self.db = {}
        # Counter for values
        self.value_count = {}
        # Transaction stack
        self.transactions = []
        # Thread lock for synchronization
        self._lock = threading.Lock()

    def _update_value_count(self, old_value, new_value):
        """Helper method to update value counts"""
        if old_value is not None:
            self.value_count[old_value] = self.value_count.get(old_value, 0) - 1
            if self.value_count[old_value] == 0:
                del self.value_count[old_value]
        
        if new_value is not None:
            self.value_count[new_value] = self.value_count.get(new_value, 0) + 1

    def set(self, name, value):
        """Set the variable name to value"""
        with self._lock:
            old_value = self.db.get(name)
            
            if self.transactions:
                # If in a transaction, store the old value for potential rollback
                current_transaction = self.transactions[-1]
                if name not in current_transaction:
                    current_transaction[name] = old_value
            
            self.db[name] = value
            self._update_value_count(old_value, value)

    def get(self, name):
        """Get the value of variable name"""
        with self._lock:
            return self.db.get(name, 'NULL')

    def unset(self, name):
        """Unset the variable name"""
        with self._lock:
            if name in self.db:
                old_value = self.db[name]
                
                if self.transactions:
                    # If in a transaction, store the old value for potential rollback
                    current_transaction = self.transactions[-1]
                    if name not in current_transaction:
                        current_transaction[name] = old_value
                
                del self.db[name]
                self._update_value_count(old_value, None)

    def numequalto(self, value):
        """Return number of variables set to value"""
        with self._lock:
            return self.value_count.get(value, 0)

    def begin(self):
        """Begin a new transaction block"""
        with self._lock:
            self.transactions.append({})

    def rollback(self):
        """Rollback the most recent transaction"""
        with self._lock:
            if not self.transactions:
                return 'NO TRANSACTION'

            transaction = self.transactions.pop()
            # Restore old values
            for name, old_value in transaction.items():
                current_value = self.db.get(name)
                if old_value is None:
                    if name in self.db:
                        del self.db[name]
                        self._update_value_count(current_value, None)
                else:
                    self.db[name] = old_value
                    self._update_value_count(current_value, old_value)

    def commit(self):
        """Commit all open transactions"""
        with self._lock:
            if not self.transactions:
                return 'NO TRANSACTION'
            self.transactions.clear()

def main():
    db = InMemoryDB()
    
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
            elif cmd == 'UNSET' and len(command) == 2:
                db.unset(command[1])
            elif cmd == 'NUMEQUALTO' and len(command) == 2:
                print(db.numequalto(command[2]))
            elif cmd == 'BEGIN':
                db.begin()
            elif cmd == 'ROLLBACK':
                result = db.rollback()
                if result:
                    print(result)
            elif cmd == 'COMMIT':
                result = db.commit()
                if result:
                    print(result)
            else:
                print('Invalid command')

        except EOFError:
            break

if __name__ == '__main__':
    main()