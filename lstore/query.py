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
        pass
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

   def insert(self, *columns):
        #check whether need add new page range
        if len(self.table.page_range)==0:
            self.table.new_page_range()
        elif self.table.page_range_list[-1].has_capacity() == False:
            self.table.new_page_range()
        #check whether need add new base page
        if len(self.table.page_range.base_page)==0:
            self.table.page_range_list[-1].new_base_page()
        elif self.table.page_range_list[-1].base_page[-1].has_capacity() == False:
            self.table.page_range_list[-1].new_base_page()
        
        #Write valule into physical location
        for i in range(num_columns):
            rid = self.table.page_range_list[-1].b_write(columns[i])
        #Plug value into directory
        #Only have primary key correspond to rid
        self.table.page_directory[columns[self.key]] = rid
            
        '''
        #check whether need initialized directory
        if len(self.table.page_directory)==0:
            for i in range(num_columns):
                self.table.page_directory[i] = {}
                
        #Write valule into physical location
        #Plug value into directory nesting dictionary
        for i in range(num_columns):
            rid = self.table.page_range_list[-1].b_write(columns[i])
            if columns[i] not in self.table.page_directory[i]:
                self.table.page_directory[i][columns[i]] = [rid]
            else:
                self.table.page_directory[i][columns[i]].append(rid)
        '''
        
        Schema_Encoding = '0' * self.table.num_columns
        

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_key, column, query_columns):
        rid = self.table.page_directory[index_key]
        [Page_Range,Page,Row,IsBasePage] = self.table.directory(rid)
        return_list = []
        for i in range(num_columns):
            if query_columns[i] == 1:
                return_list.append(self.table.page_range_list[Page_Range].b_read(Page,Row,i))
        return return_list
        
        '''
        #nesting dictionary version
        rid_list = self.table.page_directory[column][index_key]
        #2D array with all return records
        return_list = []
        for rid in rid_list
            temp_list = []
            [Page_Range,Page,Row,IsBasePage] = self.table.directory(rid)
            for i in range(num_columns):
                if query_columns[i] == 1:
                    temp_list.append(self.table.page_range_list[Page_Range].b_read(Page,Row,i))
            return_list.append(temp_list)
        return return_list
        '''
        
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        pass

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
        for i in range(start_range,end_range):
            rid = self.table.page_directory[i]
            [Page_Range,Page,Row,IsBasePage] = self.table.directory(rid)
            sum += self.table.Page_Range_list[Page_Range].b_read(Page,Row,aggregate_column_index)
        return return_sum
    
    '''
        return_sum = 0
        for i in range(start_range,end_range):
            #note: primary key cannot repeat, rid_list is a length 1 list
            rid_list = self.table.page_directory[self.table.key][i]
            [Page_Range,Page,Row,IsBasePage] = self.table.directory(rid_list[0])
            sum += self.table.Page_Range_list[Page_Range].b_read(Page,Row,aggregate_column_index)
        return return_sum
    '''  

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
