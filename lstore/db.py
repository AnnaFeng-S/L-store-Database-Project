from lstore.table import Table
from lstore.merge_thread import ThreadPool
import threading

class Database():

    def __init__(self):
        self.tables = []
        self.table_directory = {}
        self.pool = ThreadPool()
        self.lock = threading.Lock()
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        self.pool.wail_complete()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        print("Table " + name + " created")
        table = Table(name, num_columns, key_index, self.pool, self.lock)
        self.tables.append(table)
        self.table_directory[name] = len(self.tables) - 1
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        print("Table " + name + " dropped")
        self.tables.pop(self.table_directory[name])

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[self.table_directory[name]]

