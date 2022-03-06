"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from xmlrpc.client import MAXINT

class Index:

    def __init__(self, table):
        self.table_name = table.name
        self.table_num_columns = table.num_columns
        self.table_key = table.key
        self.table_rid = table.rid
        self.table_page_range = 0
        self.table = table
        self.indices = [None] *  table.num_columns
        self.indices[self.table_key] = {}

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        try:
            return self.indices[column][value]
        except:
            return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        col_dict = self.indices[column]
        if col_dict is None:
            return None
        rids = []
        for k in range(begin, end+1):
            if k in col_dict.keys():
                rids += col_dict[k]
        return rids
        

    """
    # optional: Create index on specific column
    """

    def create_index(self, column):
        # Already created
        if self.indices[column] is not None:
            return
        else:
            self.indices[column] = {}
            # Get all keys
            key_list = self.indices[self.table_key].keys()
            for key in key_list:
                rid = self.indices[self.table_key][key][0]
                [Page_Range, Page, Row] = self.table.directory[rid]
                self.table.lock.acquire()
                if [self.table.name, Page_Range] in self.table.bufferpool.bufferpool_list:
                    temp_page_range = self.table.bufferpool.bufferpool[
                        self.table.bufferpool.bufferpool_list.index([self.table.name, Page_Range])]
                elif self.table.bufferpool.has_capacity() == True:
                    self.table.bufferpool.bufferpool_list.append([self.table.name, Page_Range])
                    temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                    self.table.bufferpool.bufferpool.append(temp_page_range)
                else:
                    temp_index = self.table.bufferpool.min_used_time()
                    #if self.table.bufferpool.bufferpool[temp_index].dirty == 1:
                    self.table.bufferpool.memory_to_disk(temp_index)
                    temp_page_range = self.table.bufferpool.disk_to_memory(self.table.name, Page_Range)
                    self.table.bufferpool.bufferpool_list[temp_index] = [self.table.name, Page_Range]
                    self.table.bufferpool.bufferpool[temp_index] = temp_page_range
                record = temp_page_range.b_read_index_col(Page, Row, column)
                self.table.lock.release()
                self.insert(column, record, rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        self.indices[column] = None
        
    """ 
    # Insert New Record
    """
    def insert(self, column, value, rid):
        col_dict = self.indices[column]
        if col_dict is not None:
            if value in col_dict.keys():
                col_dict[value].append(rid)
            else:
                col_dict[value] = [rid]
        else:
            print("The index for column", column, "has not been created.")


    """ 
    # Delete Record
    """
    def delete(self, column, value, rid):
        col_dict = self.indices[column]
        if value in col_dict.keys():
            if len(col_dict[value]) == 1:
                del col_dict[value]
            else:
                col_dict[value].remove(rid)
        else:
            print("Delete index failed. The value cannot been found.")


    """ 
    # Update record with this
    """
    def update(self, column, old_value, new_value, rid):
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)