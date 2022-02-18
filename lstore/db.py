from lstore.table import Table
import pickle

class bufferpool_page_range:
    
    def __init__(self, page_range):
        self.page_range = page_range
        self.dirty = 0
        self.used_time = 0
        self.pin = 0
        
class BufferPool:
    
    def __init__(self):
        self.bufferpool = []
        self.bufferpool_list = []

    def load_data(self,table_name):
        f = open(table_name+'.pickle','rb')
        return_table = pickle.load(f)
        f.close()
        return return_table
    
    def write_data(self,table_name,data):
        f = open(table_name+'.pickle','wb')
        pickle.dump(data,f)
        f.close()
        
    def min_used_time(self):
        Min = float("inf")
        for page in self.bufferpool:
            if page.used_time < Min:
                Min = page.used_time
                return_value = self.bufferpool.index(page)
        return return_value


class Database():

    def __init__(self):
        self.tables = []
        self.table_directory = {}
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass
    
    

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        print("Table " + name + " created")
        table = Table(name, num_columns, key_index)
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
