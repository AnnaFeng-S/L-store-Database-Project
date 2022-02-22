from lstore.table import Table
import pickle
import os

        
class BufferPool:
    
    def __init__(self):
        self.bufferpool = []
        self.bufferpool_list = []
        self.path = ''
        
    def min_used_time(self):
        Min = float("inf")
        for page in self.bufferpool:
            if page.used_time < Min:
                Min = page.used_time
                return_value = self.bufferpool.index(page)
        return return_value
    
    def has_capacity(self):
        if len(self.bufferpool)>=3:
            return False
        else:
            return True
        
    def set_path(self, path):
        self.path = path
        
    def memory_to_disk(self,index):
        pass
    
    def disk_to_memory(self,table_name, page_range):
        pass
    
    

class Database():

    def __init__(self):
        self.tables = []
        self.table_directory = {}
        self.bufferpool = BufferPool()
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        self.bufferpool.set_path(path)
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)

    def close(self):
        for table in self.tables:
            f = open(self.path+table.name+'_directory','wb')
            pickle.dump(table.directory,f)
            f.close()
            f = open(self.path+table.name+'_index','wb')
            pickle.dump(table.index,f)
            f.close()
        for i in range(self.bufferpool.bufferpool):
            if self.bufferpool.bufferpool[i].dirty == 1:
                self.bufferpool.memory_to_disk(i)


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        print("Table " + name + " created")
        table = Table(name, num_columns, key_index, self.bufferpool)
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
        f = open(self.path+name+'_index','rb')
        index = pickle.load(f)
        f.close()
        table = Table(name, index.table_num_columns, index.table_key)
        table.index = index
        f = open(self.path+name+'_directory','rb')
        directory = pickle.load(f)
        f.close()
        table.directory = directory
        self.tables.append(table)
        self.table_directory[name] = len(self.tables) - 1
        return return_table
        