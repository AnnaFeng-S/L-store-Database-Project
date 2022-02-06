from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page

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
        rid = self.table.index.locate(0, primary_key)[0]
        [Page_Range,Page,Row] = self.table.directory[rid]
        self.table.page_range_list[Page_Range].b_delete(Page,Row)
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(self.table.page_range_list)==0:
            self.table.new_page_range()
        elif self.table.page_range_list[-1].has_capacity() == False:
            self.table.new_page_range()
        values = []
        for i in range(0, len(columns)):
            values.append(columns[i])
        [rid, page_index, index] = self.table.page_range_list[-1].b_write(values)
        self.table.index.insert(self.table.key, columns[0], rid)
        self.table.directory[rid] = [len(self.table.page_range_list)-1, page_index, index]



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
        for rid in rids:
            [Page_Range,Page,Row] = self.table.directory[rid]
            record = self.table.page_range_list[Page_Range].b_read(Page,Row)
            if record is None:
                continue
            columns = []
            for i in range(self.table.num_columns):
                if query_columns[i] == 1:
                    columns.append(record[i])
            return_list.append(Record(rid, 0, columns))
        return return_list
        
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid = self.table.index.locate(0, primary_key)[0]
        [Page_Range,Page,Row] = self.table.directory[rid]
        if(self.table.page_range_list[Page_Range].tail_has_capacity() == False):
            self.table.new_tail_page(Page_Range)
        self.table.page_range_list[Page_Range].t_update(Page,Row,columns)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        return_sum = 0
        rids = self.table.index.locate_range(start_range, end_range, 0)
        for rid in rids:
            [Page_Range,Page,Row] = self.table.directory[rid]
            record = self.table.page_range_list[Page_Range].b_read(Page,Row)
            return_sum += record[aggregate_column_index]
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
