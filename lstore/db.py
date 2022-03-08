from lstore.table import Table
from lstore.page_range import Page_Range
from lstore.bt_page import Base_Page, Tail_Page
import pickle
import os
from lstore.merge_thread import ThreadPool
import threading
from lstore.log import Log
        
class BufferPool:
    
    def __init__(self):
        self.bufferpool = []
        self.bufferpool_list = []
        self.path = ''
        
    def min_used_time(self):
        Min = float("inf")
        for page in self.bufferpool:
            if page.used_time < Min and page.pin == 0:
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
        os.chdir(self.path)

    def memory_to_disk(self, index):
        Last_saved_base = self.bufferpool[index].meta.Last_saved_base
        Last_saved_tail = self.bufferpool[index].meta.Last_saved_tail
        if self.bufferpool[index].dirty == 1:
            for b_index in range(len(self.bufferpool[index].base_page)):
                f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle", "wb")
                pickle.dump(self.bufferpool[index].base_page[b_index].meta_data, f)
                os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle")
                f.close()
                for col_index in range(len(self.bufferpool[index].base_page[b_index].physical_page)):
                    with open(self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                        binary_file.write(self.bufferpool[index].base_page[b_index].physical_page[col_index].data)
                        os.path.join(self.path,
                                     self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt")
        else:
            for b_index in range(0, Last_saved_base):
                if self.bufferpool[index].base_page[b_index].dirty == 1:
                    for col_index in range(len(self.bufferpool[index].base_page[b_index].physical_page)):
                        f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle", "wb")
                        pickle.dump(self.bufferpool[index].base_page[b_index].meta_data, f)
                        os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle")
                        f.close()
            for b_index in range(Last_saved_base, len(self.bufferpool[index].base_page)):
                f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle", "wb")
                pickle.dump(self.bufferpool[index].base_page[b_index].meta_data, f)
                os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_base_"+ str(b_index) + "_meta.pickle")
                f.close()
                for col_index in range(len(self.bufferpool[index].base_page[b_index].physical_page)):
                    with open(self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                        binary_file.write(self.bufferpool[index].base_page[b_index].physical_page[col_index].data)
                        os.path.join(self.path, self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_basepage_"+str(b_index)+"_col_"+str(col_index)+".txt")
        for t_index in range(0, Last_saved_tail):
            if self.bufferpool[index].tail_page[t_index].dirty == 1:
                f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tail_"+ str(t_index) + "_meta.pickle", "wb")
                pickle.dump(self.bufferpool[index].tail_page[t_index].meta_data, f)
                os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tail_"+ str(t_index) + "_meta.pickle")
                f.close()
        for t_index in range(Last_saved_tail, len(self.bufferpool[index].tail_page)):
            f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tail_"+ str(t_index) + "_meta.pickle", "wb")
            pickle.dump(self.bufferpool[index].tail_page[t_index].meta_data, f)
            os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_tail_"+ str(b_index) + "_meta.pickle")
            f.close()
            for col_index in range(len(self.bufferpool[index].tail_page[t_index].physical_page)):
                with open(self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                    binary_file.write(self.bufferpool[index].tail_page[t_index].physical_page[col_index].data)
                    os.path.join(self.path,self.bufferpool_list[index][0]+"_"+str(self.bufferpool_list[index][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt")
        self.bufferpool[index].meta.Last_saved_base = len(self.bufferpool[index].base_page) - 1
        self.bufferpool[index].meta.Last_saved_tail = len(self.bufferpool[index].tail_page) - 1
        if(self.bufferpool[index].meta.Last_saved_base == -1):
            self.bufferpool[index].meta.Last_saved_base = 0
        if(self.bufferpool[index].meta.Last_saved_tail == -1):
            self.bufferpool[index].meta.Last_saved_tail = 0
        f = open(self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_page_range_meta.pickle", "wb")
        pickle.dump(self.bufferpool[index].meta, f)
        os.path.join(self.path,self.bufferpool_list[index][0] + "_" + str(self.bufferpool_list[index][1]) + "_page_range_meta.pickle")
        f.close()
                    
    def disk_to_memory(self, table_name, page_range):
        #os.chdir(self.path)
        #print("Disk to memory: ", page_range)
        f = open(table_name + "_" + str(page_range) + "_page_range_meta.pickle", "rb")
        page_range_metadata = pickle.load(f)
        f.close()
        return_page_range = Page_Range(page_range_metadata.n_columns,page_range_metadata.next_brid,page_range_metadata.next_trid,page_range_metadata.tail_block_size)
        return_page_range.meta.next_bpage = page_range_metadata.next_bpage
        return_page_range.meta.next_tpage = page_range_metadata.next_tpage
        return_page_range.meta.trid_list = page_range_metadata.trid_list
        return_page_range.meta.Last_saved_base = page_range_metadata.Last_saved_base
        return_page_range.meta.Last_saved_tail = page_range_metadata.Last_saved_tail
        #print("Disk to memory: ", return_page_range.meta.merge_time)
        for b_index in range(page_range_metadata.next_bpage):
            return_page_range.base_page.append(Base_Page(return_page_range.meta.n_columns))
            f = open(table_name + "_" + str(page_range)  + "_base_"+ str(b_index) + "_meta.pickle", "rb")
            b_metadata = pickle.load(f)
            return_page_range.base_page[b_index].meta_data = b_metadata
            f.close()
            for col_index in range(page_range_metadata.n_columns):
                #return_page_range.base_page[b_index].physical_page.append(Base_Page.Physical_Page())
                f = open(table_name + "_" + str(page_range) + "_basepage_" + str(b_index) + "_col_" + str(col_index) + ".txt", "rb")
                return_page_range.base_page[b_index].physical_page[col_index].data = bytearray(f.read())
                return_page_range.base_page[b_index].physical_page[col_index].num_records = return_page_range.base_page[b_index].meta_data.num_records
                f.close()
        for t_index in range(page_range_metadata.next_tpage):
            return_page_range.tail_page.append(Tail_Page(return_page_range.meta.n_columns))
            f = open(table_name + "_" + str(page_range)  + "_tail_"+ str(t_index) + "_meta.pickle", "rb")
            t_metadata = pickle.load(f)
            return_page_range.tail_page[t_index].meta_data = t_metadata
            f.close()
            for col_index in range(page_range_metadata.n_columns):
                f = open(table_name + "_" + str(page_range) + "_tailpage_" + str(t_index) + "_col_" + str(col_index) + ".txt", "rb")
                return_page_range.tail_page[t_index].physical_page[col_index].data = bytearray(f.read())
                return_page_range.tail_page[t_index].physical_page[col_index].num_records = return_page_range.tail_page[t_index].meta_data.num_records
                f.close()
        return return_page_range
    

class Database():

    def __init__(self):
        self.tables = []
        self.table_directory = {}
        self.bufferpool = BufferPool()
        self.path = ''
        self.pool = ThreadPool()
        self.log = Log()
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.bufferpool.set_path(path)
        

    def close(self):
        self.pool.wait_complete()
        for table in self.tables:
            f = open(table.name+'_directory','wb')
            pickle.dump(table.directory,f)
            os.path.join(table.name+'_directory')
            f.close()
            table.index.rid = table.rid
            table.index.table_page_range = table.page_range_num
            table.index.table = None
            f = open(table.name+'_index','wb')
            pickle.dump(table.index,f)
            os.path.join(table.name+'_index')
            f.close()
        for i in range(len(self.bufferpool.bufferpool)):
            self.bufferpool.memory_to_disk(i)


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        print("Table " + name + " created")
        table = Table(name, num_columns, key_index, self.pool, self.bufferpool,self.log)
        self.tables.append(table)
        self.table_directory[name] = len(self.tables) - 1
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        file_name = name
        print("Table " + name + " dropped")
        check_route = "./" + name + "_"
        if name in self.table_directory:
            self.tables.pop(self.table_directory[name])
            self.table_directory.pop(name)
        for root, dirs, files in os.walk("."):
            for name in files:
                route = os.path.join(root, name)
                if check_route in route:
                    os.remove(route)
        keys = self.bufferpool.bufferpool_list.copy()
        i = 0
        counter = 0
        for key in keys:
            if file_name == key[0]:
                self.bufferpool.bufferpool.pop(i-counter)
                self.bufferpool.bufferpool_list.remove(key)
                counter += 1
            i += 1


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        f = open(name+'_index','rb')
        index = pickle.load(f)
        f.close()
        table = Table(name, index.table_num_columns, index.table_key, self.pool, self.bufferpool, self.log)
        table.index = index
        table.rid = table.index.rid
        table.index.table = table
        table.page_range_num = table.index.table_page_range
        f = open(name+'_directory','rb')
        directory = pickle.load(f)
        f.close()
        table.directory = directory
        self.tables.append(table)
        self.table_directory[name] = len(self.tables) - 1
        return table
        