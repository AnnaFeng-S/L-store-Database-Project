import random
import time
import threading

class Singleton(type):
    def __init__(self, *args, **kwargs):
        self.__instance = None

    def __call__(self, *args, **kwargs):
        if not self.__instance:
            self.__instance = super().__call__(*args, **kwargs)
        return self.__instance

class TwoPhaseLock:
    def __init__(self):
        self.xlock = 0
        self.slock = 0
        self.xlock_owner = None
        self.list = []

class LockManager(metaclass=Singleton):
    time = time.time()
    lock = threading.Lock()
    lock_list = {}
    transaction_status = {}
    transaction_locks = {}

    def acquire_xlock(self, record, transaction_id):
        print(transaction_id, " acquire xlock ", record)
        if transaction_id in self.transaction_status:
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        if not (record in self.lock_list):
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.xlock += 1
            lock_entity.xlock_owner = transaction_id
        else:
            lock_entity = self.lock_list[record]
            if lock_entity.xlock != 0 and lock_entity.xlock_owner != transaction_id:
                return False
            ## Check
            if (lock_entity.slock != 0 or len(lock_entity.list) != 1) and lock_entity.list[0] != transaction_id:
                return False
            else:
                if lock_entity.xlock == 0:
                    lock_entity.xlock += 1
                    lock_entity.xlock_owner = transaction_id
                    if transaction_id in self.transaction_locks:
                        self.transaction_locks[transaction_id].append([record, "xlock"])
                    else:
                        self.transaction_locks[transaction_id] = [[record, "xlock"]]
        self.transaction_status[transaction_id] = "expanding"
        return True
    
    def acquire_slock(self, record, transaction_id):
        if transaction_id in self.transaction_status:
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        if not (record in self.lock_list):
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.slock += 1
            lock_entity.list.append(transaction_id)
        else:
            lock_entity = self.lock_list[record]
            if lock_entity.xlock != 0:
                return False
            else:
                if not (transaction_id in lock_entity.list):
                    lock_entity.list.append(transaction_id)
                    lock_entity.slock += 1
                    if transaction_id in self.transaction_locks:
                        self.transaction_locks[transaction_id].append([record, "slock"])
                    else:
                        self.transaction_locks[transaction_id] = [[record, "slock"]]
        self.transaction_status[transaction_id] = "expanding"
        return True

    def release_locks(self, transaction_id):
        if not (transaction_id in self.transaction_locks):
            return
        locks = self.transaction_locks[transaction_id]
        self.transaction_status[transaction_id] = "shrinking"
        for lock in locks:
            lock_entity = self.lock_list[lock[0]]
            if lock[1] == "xlock":
                lock_entity.xlock -= 1
                lock_entity.xlock_owner = None
            else:
                lock_entity.slock -= 1
                lock_entity.list.remove(transaction_id)
        del self.transaction_locks[transaction_id]
        del self.transaction_status[transaction_id]
        return True

