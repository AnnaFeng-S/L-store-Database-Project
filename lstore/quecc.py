from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.transaction import Transaction
import threading, queue, time

class QueCC:
    def __init__(self, number_of_threads = 1):
        self.stats = []
        self.result = 0
        self.high_priority_queue = []
        self.high_priority_table = []
        self.high_priority_table_list = []
        self.low_priority_table_list = []
        self.low_priority_table = []
        self.low_priority_queue = []
        self.initial_queue = []
        self.current_queue = 0
        self.number_of_threads = number_of_threads
        for i in range(number_of_threads):
            self.high_priority_queue.append([])
            self.low_priority_queue.append([])
            self.high_priority_table.append([])
            self.low_priority_table.append([])
            self.high_priority_table_list.append([])
            self.low_priority_table_list.append([])
        for i in range(0,2):
            self.initial_queue.append([])
        self.lock = threading.Lock()
        pass

    def work(self):
        self.plan()
        
        finbal_table = []
        for i in range(self.number_of_threads):
            combined_table = self.high_priority_table[i] + self.low_priority_table[i]
            finbal_table.append(combined_table)
        # get list of execution threads
        execution_threads = []
        for i in range(self.number_of_threads):
            execution_threads.append(ExecutionThread(self.high_priority_queue[i], self.low_priority_queue[i], self.high_priority_table_list[i], self.low_priority_table_list[i],finbal_table[i]))
        # start execution threads
        for i in range(self.number_of_threads):
            execution_threads[i].run()
        # join execution threads
        for i in range(self.number_of_threads):
            execution_threads[i].join()
        
    
    def add_transaction(self, t):
        self.initial_queue[self.current_queue].append(t)
        self.current_queue += 1
        if self.current_queue == 2:
            self.current_queue = 0
        pass

    def plan(self):
        high_priority_planner = PlanThread(self.initial_queue[0], self, "high")
        low_priority_planner = PlanThread(self.initial_queue[1], self, "low")
        high_priority_planner.run()
        low_priority_planner.run()
        high_priority_planner.join()
        low_priority_planner.join()

class PlanThread:
    def __init__(self, transactions, quecc, priority):
        self.quecc = quecc
        self.transactions = transactions
        self._queue = queue.Queue()
        self.priority = priority

    def run(self):
        for transaction in self.transactions:
            self._queue.put(transaction)
        threading.Thread(target=self.__run, daemon=True).start()
        pass
    
    def join(self):
        self._queue.join()
        pass

    def work(self):
        while 1:
            transaction = self._queue.get()
            transaction_table = transaction.table
            transaction_table_list = transaction.table_list
            for i in range(len(transaction.queries)):
                query = transaction.queries[i]
                if query[0].__name__ == 'insert':
                    args = query[1]
                    key = args[0]
                    queue_index = key % self.quecc.number_of_threads
                    self.quecc.lock.acquire()
                    if self.priority == "high":
                        self.quecc.high_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.high_priority_table[queue_index]:
                            self.quecc.high_priority_table[queue_index].append(table)
                        self.quecc.high_priority_table_list[queue_index].append(self.quecc.high_priority_table[queue_index].index(table))
                    else:
                        self.quecc.low_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.low_priority_table[queue_index]:
                            self.quecc.low_priority_table[queue_index].append(table)
                        self.quecc.low_priority_table_list[queue_index].append(self.quecc.low_priority_table[queue_index].index(table))
                    self.quecc.lock.release()
                elif query[0].__name__ == 'update':
                    args = query[1]
                    key = args[0]
                    queue_index = key % self.quecc.number_of_threads
                    self.quecc.lock.acquire()
                    if self.priority == "high":
                        self.quecc.high_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.high_priority_table[queue_index]:
                            self.quecc.high_priority_table[queue_index].append(table)
                        self.quecc.high_priority_table_list[queue_index].append(self.quecc.high_priority_table[queue_index].index(table))
                    else:
                        self.quecc.low_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.low_priority_table[queue_index]:
                            self.quecc.low_priority_table[queue_index].append(table)
                        self.quecc.low_priority_table_list[queue_index].append(self.quecc.low_priority_table[queue_index].index(table))
                    self.quecc.lock.release()
                elif query[0].__name__ == 'select':
                    args = query[1]
                    key = args[0]
                    queue_index = key % self.quecc.number_of_threads
                    self.quecc.lock.acquire()
                    if self.priority == "high":
                        self.quecc.high_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.high_priority_table[queue_index]:
                            self.quecc.high_priority_table[queue_index].append(table)
                        self.quecc.high_priority_table_list[queue_index].append(self.quecc.high_priority_table[queue_index].index(table))
                    else:
                        self.quecc.low_priority_queue[queue_index].append(query)
                        index = transaction_table_list[i]
                        table = transaction_table[index]
                        if table not in self.quecc.low_priority_table[queue_index]:
                            self.quecc.low_priority_table[queue_index].append(table)
                        self.quecc.low_priority_table_list[queue_index].append(self.quecc.low_priority_table[queue_index].index(table))
                    self.quecc.lock.release()
            self._queue.task_done()
    
    def __run(self):
        for transaction in self.transactions:
            self.work()


class ExecutionThread:
    def __init__(self, high_queries, low_queries, high_table_list, low_table_list, table):
        self.high_transaction = Transaction()
        for i in range(len(high_queries)):
            self.high_transaction.add_query(high_queries[i][0], table[high_table_list[i]], high_queries[i][1])
        self.low_transaction = Transaction()
        for i in range(len(low_queries)):
            self.low_transaction.add_query(low_queries[i][0], table[low_table_list[i]], low_queries[i][1])
        self.high_transaction.queries = high_queries
        self.low_transaction.queries = low_queries
        self._queue = queue.Queue()
        self._queue.put(self.high_transaction)
        self._queue.put(self.low_transaction)
        pass

    def run(self):
        threading.Thread(target=self.__run, daemon=True).start()
        pass

    def join(self):
        self._queue.join()
        pass

    def work(self):
        while 1:
            transaction = self._queue.get()
            transaction.run()
            self._queue.task_done()

    def __run(self):
        self.work()