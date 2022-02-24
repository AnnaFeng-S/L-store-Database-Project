from lstore.table import Table
from lstore.page_range import Page_Range
from lstore.bt_page import Base_Page, Tail_Page
import pickle
import os
from lstore.merge_thread import ThreadPool
import threading
        
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

    def memory_to_disk(self, index):
        #os.chdir(self.path)
        f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_page_range_meta.pickle",
                 "wb")
        pickle.dump(self.bufferpool[index].meta, f)
        os.path.join(self.path,
                     self.bufferpool_list[index][0] + "_" + str(
                         self.bufferpool_list[index][1]) + "_page_range_meta.pickle")
        f.close()
        for b_index in range(len(self.bufferpool[index].base_page)):
            f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_basemeta.pickle", "wb")
            pickle.dump(self.bufferpool[index].base_page[b_index].meta_data, f)
            os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_basemeta.pickle")
            f.close()
            for col_index in range(len(self.bufferpool[index].base_page[b_index].physical_page)):
                with open(self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                    binary_file.write(self.bufferpool[index].base_page[b_index].physical_page[col_index].data)
                    os.path.join(self.path,
                                 self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt")
        for t_index in range(len(self.bufferpool[index].tail_page)):
            f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tailmeta.pickle", "wb")
            pickle.dump(self.bufferpool[index].tail_page[t_index].meta_data, f)
            os.path.join(self.path,
                         self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tailmeta.pickle")
            f.close()
            for col_index in range(len(self.bufferpool[index].tail_page[t_index].physical_page)):
                with open(self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                    binary_file.write(self.bufferpool[index].tail_page[t_index].physical_page[col_index].data)
                    os.path.join(self.path,
                                 self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt")

    def disk_to_memory(self, table_name, page_range):
        #os.chdir(self.path)
        f = open(table_name + "_" + str(page_range) + "_page_range_meta.pickle", "rb")
        page_range_metadata = pickle.load(f)
        f.close()
        return_page_range = Page_Range(page_range_metadata.n_columns,page_range_metadata.next_brid,page_range_metadata.next_trid,page_range_metadata.tail_block_size)
        return_page_range.meta.next_bpage = page_range_metadata.next_bpage
        return_page_range.meta.next_tpage = page_range_metadata.next_tpage
        return_page_range.meta.trid_list = page_range_metadata.trid_list
        return_page_range.meta.merge_time = page_range_metadata.merge_time
        for b_index in range(page_range_metadata.next_bpage):
            return_page_range.base_page.append(Base_Page(return_page_range.meta.n_columns))
            f = open(table_name + "_" + str(page_range) + "_basemeta.pickle", "rb")
            b_metadata = pickle.load(f)
            return_page_range.base_page[b_index].meta_data = b_metadata
            f.close()
            for col_index in range(page_range_metadata.n_columns):
                #return_page_range.base_page[b_index].physical_page.append(Base_Page.Physical_Page())
                f = open(table_name + "_" + str(page_range) + "_basepage_" + str(b_index) + "_col_" + str(col_index) + ".txt", "rb")
                return_page_range.base_page[b_index].physical_page[col_index].data = f.read()
                return_page_range.base_page[b_index].physical_page[col_index].num_records = return_page_range.base_page[b_index].meta_data.num_records
                f.close()
        for t_index in range(page_range_metadata.next_tpage):
            return_page_range.tail_page.append(Tail_Page(return_page_range.meta.n_columns))
            for col_index in range(page_range_metadata.n_columns):
                f = open(table_name + "_" + str(page_range) + "_tailpage_" + str(t_index) + "_col_" + str(col_index) + ".txt", "rb")
                return_page_range.tail_page[t_index].physical_page[col_index].data = f.read()
            f = open(table_name + "_" + str(page_range) + "_tailmeta.pickle", "rb")
            t_metadata = pickle.load(f)
            return_page_range.tail_page[t_index].meta_data = t_metadata
            f.close()
        return return_page_range
    

class Database():

    def __init__(self):
        self.tables = []
        self.table_directory = {}
        self.bufferpool = BufferPool()
        self.path = ''
        self.pool = ThreadPool()
        self.lock = threading.Lock()
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        self.bufferpool.set_path(path)
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)

    def close(self):
        os.chdir(self.path)
        self.pool.wail_complete()
        for table in self.tables:
            f = open(table.name+'_directory','wb')
            pickle.dump(table.directory,f)
            os.path.join(table.name+'_directory')
            f.close()
            f = open(table.name+'_index','wb')
            pickle.dump(table.index,f)
            os.path.join(table.name+'_index')
            f.close()
        for i in range(len(self.bufferpool.bufferpool)):
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
        table = Table(name, num_columns, key_index, self.pool, self.lock, self.bufferpool)
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
        f = open(name+'_index','rb')
        index = pickle.load(f)
        f.close()
        table = Table(name, index.table_num_columns, index.table_key, self.pool, self.lock, self.bufferpool)
        table.index = index
        f = open(name+'_directory','rb')
        directory = pickle.load(f)
        f.close()
        table.directory = directory
        self.tables.append(table)
        self.table_directory[name] = len(self.tables) - 1
        return table
        