from lstore.table import Table, Record
from lstore.index import Index
import threading, queue, time

class TwoPhaseLock:
    def __init__(self):
        self.xlock = 0
        self.slock = 0
        self.xlock_owner = None
        self.list = []

class LockManager:
    def __init__(self):
        print("LockManager init")
        self.time = time.time()
        self.lock = threading.Lock()
        self.lock_list = {}
        self.transaction_status = {}
        self.transaction_locks = {}
    
    def acquire_xlock(self, record, transaction_id):
        if self.transaction_status.contains(transaction_id):
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        lock_entity = self.lock_list[record]
        if lock_entity == None:
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.xlock += 1
            lock_entity.xlock_owner = transaction_id
        else:
            if lock_entity.xlock != 0 or lock_entity.slock != 0:
                return False
            else:
                lock_entity.xlock += 1
                lock_entity.xlock_owner = transaction_id
        
        self.transaction_status[transaction_id] = "expanding"
        if transaction_locks.contains(transaction_id):
            transaction_locks[transaction_id] += 1
        else:
            transaction_locks[transaction_id] = 1
        return True
    
    def acquire_slock(self, record, transaction_id):
        if self.transaction_status.contains(transaction_id):
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        lock_entity = self.lock_list[record]
        if lock_entity == None:
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.slock += 1
            lock_entity.list.append(transaction_id)
        else:
            if lock_entity.xlock != 0:
                return False
            else:
                lock_entity.slock += 1
                lock_entity.list.append(transaction_id)

        self.transaction_status[transaction_id] = "expanding"
        if transaction_locks.contains(transaction_id):
            transaction_locks[transaction_id] += 1
        else:
            transaction_locks[transaction_id] = 1
        return True

    def release_xlock(self, record, transaction_id):
        lock_entity = self.lock_list[record]
        if lock_entity == None:
            return False
        else:
            if lock_entity.xlock == 0 or lock_entity.xlock_owner != transaction_id:
                return False
            else:
                lock_entity.xlock -= 1
                if lock_entity.xlock == 0:
                    lock_entity.xlock_owner = None
        self.transaction_status[transaction_id] = "shrinking"
        transaction_locks[transaction_id] -= 1
        if transaction_locks[transaction_id] == 0:
            del transaction_status[transaction_id]
        return True

    def release_slock(self, record, transaction_id):
        lock_entity = self.lock_list[record]
        if lock_entity == None:
            return False
        else:
            if lock_entity.slock == 0 or lock_entity.list.contains(transaction_id) == False:
                return False
            else:
                lock_entity.slock -= 1
                lock_entity.list.remove(transaction_id)
        self.transaction_status[transaction_id] = "shrinking"
        transaction_locks[transaction_id] -= 1
        if transaction_locks[transaction_id] == 0:
            del transaction_status[transaction_id]
        return True

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions
        self.stats = []
        self.result = 0
        self._queue = queue.Queue()
        self.transaction_index = 0
        self.worker_thread = 0
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
        pass

    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        for transaction in self.transactions:
            self._queue.put(transaction)
        threading.Thread(target=self.__run, daemon=True).start()
        pass

    """
    Waits for the worker to finish
    """
    def join(self):
        self._queue.join()
        pass

    def work(self):
        while 1:
            transaction = self._queue.get()
            transaction.transaction_id = self.worker_thread + "_" + str(self.transaction_index)
            self.transaction_index += 1
            self.stats.append(transaction.run())
            self._queue.task_done()

    def __run(self):
        # get thread id
        self.worker_thread = str(threading.current_thread().ident)
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.work()
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
