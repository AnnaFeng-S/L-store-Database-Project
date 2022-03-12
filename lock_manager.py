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
        #print("Acquire xlock for ", record, " by ", transaction_id)
        if transaction_id in self.transaction_status:
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        if not (record in self.lock_list):
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.xlock += 1
            lock_entity.xlock_owner = transaction_id
            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].append([record, "xlock"])
                #print(self.transaction_locks)
            else:
                self.transaction_locks[transaction_id] = [[record, "xlock"]]
                #print(self.transaction_locks)
        else:
            lock_entity = self.lock_list[record]
            # If transaction already have the xlock, it's okay
            if lock_entity.xlock != 0 and lock_entity.xlock_owner == transaction_id:
                return True
            # If xlock is hold by another transaction, it's not okay
            if lock_entity.xlock != 0 and lock_entity.xlock_owner != transaction_id:
                #print("Reason 1")
                return False
            if lock_entity.slock > 1:
                #print("Reason 2")
                return False
            ## If slock exists in this record, and it's not hold by this transaction, it's not okay
            if (lock_entity.slock == 1 and transaction_id not in lock_entity.list):
                #print("Reason 3")
                return False
            lock_entity.xlock += 1
            lock_entity.xlock_owner = transaction_id
            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].append([record, "xlock"])
                #print(self.transaction_locks)
            else:
                self.transaction_locks[transaction_id] = [[record, "xlock"]]
                #print(self.transaction_locks)
        self.transaction_status[transaction_id] = "expanding"
        return True
    
    def acquire_slock(self, record, transaction_id):
        #print("Acquire slock for ", record, " by ", transaction_id)
        if transaction_id in self.transaction_status:
            if self.transaction_status[transaction_id] == "shrinking":
                return False
        if not (record in self.lock_list):
            lock_entity = TwoPhaseLock()
            self.lock_list[record] = lock_entity
            lock_entity.slock += 1
            lock_entity.list.append(transaction_id)
            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].append([record, "slock"])
                #print(self.transaction_locks)
            else:
                self.transaction_locks[transaction_id] = [[record, "slock"]]
                #print(self.transaction_locks)
        else:
            lock_entity = self.lock_list[record]
            # If transaction already have the slock, it's okay
            if transaction_id in lock_entity.list:
                return True
            # If xlock is hold by transaction, it's not okay
            if lock_entity.xlock != 0:
                return False
            else:
                if not (transaction_id in lock_entity.list):
                    lock_entity.list.append(transaction_id)
                    lock_entity.slock += 1
                    if transaction_id in self.transaction_locks:
                        self.transaction_locks[transaction_id].append([record, "slock"])
                        #print(self.transaction_locks)
                    else:
                        self.transaction_locks[transaction_id] = [[record, "slock"]]
                        #print(self.transaction_locks)
        self.transaction_status[transaction_id] = "expanding"
        return True

    def release_locks(self, transaction_id):
        #print("Release locks for ", transaction_id)
        #print("Transaction locks: ", self.transaction_locks)
        if not (transaction_id in self.transaction_locks):
            #print("No locks to release")
            return
        locks = self.transaction_locks[transaction_id]
        self.transaction_status[transaction_id] = "shrinking"
        for lock in locks:
            lock_entity = self.lock_list[lock[0]]
            if lock[1] == "xlock":
                lock_entity.xlock -= 1
                lock_entity.xlock_owner = None
                #print("Release xlock for ", lock[0])
            else:
                lock_entity.slock -= 1
                lock_entity.list.remove(transaction_id)
                #print("Release slock for ", lock[0])
        del self.transaction_locks[transaction_id]
        del self.transaction_status[transaction_id]
        return True

