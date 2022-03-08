from lstore.table import Table, Record
from lstore.index import Index
import threading, queue, time

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

