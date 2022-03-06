from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from lstore.db import Database
import pickle


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        rids = self.table.index.locate(0, primary_key)
        if rids is None:
            return False
        rid = rids[0]
        self.table.lock.acquire()
        [Page_Range, Page, Row] = self.table.directory[rid]
        if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
            temp_page_range = self.table.bufferpool.bufferpool[
                self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
            temp_page_range.pin += 1
        elif self.table.bufferpool.has_capacity() == True:
            self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
            temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
            self.table.bufferpool.bufferpool.append(temp_page_range)
            temp_page_range.pin += 1
        else:
            temp_index = self.table.bufferpool.min_used_time()
            self.table.bufferpool.memory_to_disk(temp_index)
            temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
            self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
            self.table.bufferpool.bufferpool[temp_index] = temp_page_range
            temp_page_range.pin += 1
        records = temp_page_range.b_read(Page, Row)
        self.table.lock.release()
        #add log information
        self.table.log.log_num += 1;
        self.table.log.method.append(1)
        self.table.log.Xact_id.append(threading.currentThread().ident)
        self.table.log.table_name.append(self.table.name)
        self.table.log.method_information.append([Page_Range, Page, Row])
        rid_list = [[rid,Page,Row]]
        indirection = temp_page_range.base_page[Page].meta_data.read_INDIRECTION(Row)
        while rid != indirection:
            [new_page_index, new_index] = temp_page_range.t_locate(indirection)
            rid_list.append([indirection,new_page_index, new_index])
            indirection = temp_page_range.tail_page[new_page_index].meta_data.read_INDIRECTION(new_index)
        self.table.log.method_meta.append(rid_list)
         
        temp_page_range.b_delete(Page, Row)
        temp_page_range.used_time += 1
        temp_page_range.base_page[Page].dirty = 1
        for i in range(self.table.num_columns):
            if self.table.index.indices[i] is not None:
                self.table.index.delete(i, records[i], rid)
        self.table.directory.pop(rid)
        temp_page_range.pin -= 1
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        check = 0
        self.table.lock.acquire()
        if self.table.page_range_num == 0:
            self.table.new_page_range()
            check = 1
        if check == 1:
            if self.table.bufferpool.has_capacity() == True:
                self.table.bufferpool.bufferpool.append(self.table.page_range_list[-1])
                self.table.bufferpool.bufferpool_list.append([self.table.name, len(self.table.page_range_list) - 1])
                temp_page_range = self.table.page_range_list[-1]
            else:
                temp_index = self.table.bufferpool.min_used_time()
                self.table.bufferpool.memory_to_disk(temp_index)
                self.table.bufferpool.bufferpool[temp_index] = self.table.page_range_list[-1]
                self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, len(self.table.page_range_list) - 1]
                temp_page_range = self.table.bufferpool.bufferpool[temp_index]
        else:
            Page_Range = self.table.page_range_num - 1
            if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
                temp_page_range = self.table.bufferpool.bufferpool[
                    self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
            elif self.table.bufferpool.has_capacity() == True:
                self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool.append(temp_page_range)
            else:
                temp_index = self.table.bufferpool.min_used_time()
                self.table.bufferpool.memory_to_disk(temp_index)
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
                self.table.bufferpool.bufferpool[temp_index] = temp_page_range
        if temp_page_range.has_capacity() == False:
            if self.table.bufferpool.has_capacity() == True:
                self.table.new_page_range()
                self.table.bufferpool.bufferpool_list.append([self.table.name, self.table.page_range_num - 1])
                self.table.bufferpool.bufferpool.append(self.table.page_range_list[-1])
                temp_page_range = self.table.page_range_list[-1]
            else:
                temp_index = self.table.bufferpool.min_used_time()
                self.table.bufferpool.memory_to_disk(temp_index)
                self.table.new_page_range()
                self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, self.table.page_range_num - 1]
                self.table.bufferpool.bufferpool[temp_index] = self.table.page_range_list[-1]
                temp_page_range = self.table.page_range_list[-1]
        temp_page_range.pin += 1
        self.table.lock.release()
        [rid, page_index, index] = temp_page_range.b_write(columns)
        #add log information
        self.table.log.log_num += 1;
        self.table.log.method.append(0)
        self.table.log.Xact_id.append(threading.currentThread().ident)
        self.table.log.table_name.append(self.table.name)
        self.table.log.method_information.append([self.table.page_range_num-1, page_index, index])
        self.table.log.method_meta.append([])
        
        temp_page_range.base_page[page_index].dirty = 1
        for i in range(len(columns)):
            if self.table.index.indices[i] is not None:
                self.table.index.insert(i, columns[i], rid)
        self.table.directory[rid] = [self.table.page_range_num - 1, page_index, index]
        temp_page_range.used_time += 1
        temp_page_range.pin -= 1
        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_key, column, query_columns):
        rids = self.table.index.locate(column, index_key)
        return_list = []
        if rids is None:
            return return_list
        for rid in rids:
            self.table.lock.acquire()
            [Page_Range, Page, Row] = self.table.directory[rid]
            if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
                temp_page_range = self.table.bufferpool.bufferpool[
                    self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
                temp_page_range.pin += 1
            elif self.table.bufferpool.has_capacity() == True:
                self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool.append(temp_page_range)
                temp_page_range.pin += 1
            else:
                temp_index = self.table.bufferpool.min_used_time()
                self.table.bufferpool.memory_to_disk(temp_index)
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
                self.table.bufferpool.bufferpool[temp_index] = temp_page_range
                temp_page_range.pin += 1
            temp_page_range.used_time += 1
            record = temp_page_range.b_read(Page, Row)
            self.table.lock.release()
            #add log information
            self.table.log.log_num += 1;
            self.table.log.method.append(3)
            self.table.log.Xact_id.append(threading.currentThread().ident)
            self.table.log.table_name.append(self.table.name)
            self.table.log.method_information.append([Page_Range, Page, Row])
            self.table.log.method_meta.append([])
            
            columns = []
            for i in range(self.table.num_columns):
                if query_columns[i] == 1:
                    columns.append(record[i])
                else:
                    columns.append(None)
            return_list.append(Record(rid, 0, columns))
            temp_page_range.pin -= 1
        return return_list

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rids = self.table.index.locate(0, primary_key)
        if rids is None:
            return False
        rid = rids[0]
        self.table.lock.acquire()
        [Page_Range, Page, Row] = self.table.directory[rid]
        if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
            temp_page_range = self.table.bufferpool.bufferpool[
                self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
            temp_page_range.pin += 1
        elif self.table.bufferpool.has_capacity() == True:
            self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
            temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
            self.table.bufferpool.bufferpool.append(temp_page_range)
            temp_page_range.pin += 1
        else:
            temp_index = self.table.bufferpool.min_used_time()
            #if self.table.bufferpool.bufferpool[temp_index].dirty == 1:
            self.table.bufferpool.memory_to_disk(temp_index)
            temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
            self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
            self.table.bufferpool.bufferpool[temp_index] = temp_page_range
            temp_page_range.pin += 1
        temp_page_range.used_time += 1
        self.table.lock.release()
        if (temp_page_range.tail_has_capacity() == False):
            self.table.new_tail_page(Page_Range)
        temp_page_range.base_page[Page].dirty = 1
        old_values = temp_page_range.b_read(Page, Row)
        temp_page_range.t_update(Page, Row, columns)
        for i in range(len(columns)):
            if self.table.index.indices[i] is not None:
                if columns[i] is not None:
                    self.table.index.update(i, old_values[i], columns[i], rid)
        temp_page_range.tail_page[-1].dirty = 1
        temp_page_range.pin -= 1
        #add log information
        self.table.log.log_num += 1;
        self.table.log.method.append(2)
        self.table.log.Xact_id.append(threading.currentThread().ident)
        self.table.log.table_name.append(self.table.name)
        self.table.log.method_information.append([Page_Range, Page, Row])
        rid_list = [[rid,Page,Row]]
        indirection = temp_page_range.base_page[Page].meta_data.read_INDIRECTION(Row)
        while rid != indirection:
            [new_page_index, new_index] = temp_page_range.t_locate(indirection)
            rid_list.append([indirection,new_page_index, new_index])
            indirection = temp_page_range.tail_page[new_page_index].meta_data.read_INDIRECTION(new_index)
        self.table.log.method_meta.append(rid_list)
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        if rids is None:
            return False
        return_sum = 0
        for rid in rids:
            [Page_Range, Page, Row] = self.table.directory[rid]
            self.table.lock.acquire()
            if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
                temp_page_range = self.table.bufferpool.bufferpool[
                    self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
                temp_page_range.pin += 1
            elif self.table.bufferpool.has_capacity() == True:
                self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool.append(temp_page_range)
                temp_page_range.pin += 1
            else:
                temp_index = self.table.bufferpool.min_used_time()
                #if self.table.bufferpool.bufferpool[temp_index].dirty == 1:
                self.table.bufferpool.memory_to_disk(temp_index)
                temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
                self.table.bufferpool.bufferpool[temp_index] = temp_page_range
                temp_page_range.pin += 1
            temp_page_range.used_time += 1
            record = temp_page_range.b_read(Page, Row)
            #add log information
            self.table.lock.release()
            self.table.log.log_num += 1;
            self.table.log.method.append(3)
            self.table.log.Xact_id.append(threading.currentThread().ident)
            self.table.log.table_name.append(self.table.name)
            self.table.log.method_information.append([Page_Range, Page, Row])
            self.table.log.method_meta.append([])
            
            return_sum += record[aggregate_column_index]
            temp_page_range.pin -= 1
        return return_sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False