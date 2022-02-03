"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.table import table
from xmlrpc.client import MAXINT
from BTrees import OOBTree


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        # rid count
        self.count_rid = [0] * table.num_columns
        self.create_index(table.key)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        try:
            col = self.indices[column]
            location =  col[value]
            return location
        except:
            return "KeyError"

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return list(self.indices[column].values(begin, end, excludemax=False))

    """
    # optional: Create index on specific column
    """

    def create_index(self, column):
        self.indices[column] = OOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column):
        self.indices[column] = None
        
    """ 
    # Insert New Record
    """
    def insert(self, column, value, rid):
        #if value is None:
        #    value = -MAXINT
        col = self.indices[column]
        if col is not None:
            self.count_rid[column] += 1
            if col.has_key(value):
                col[value].append(rid)
            else:
                col[value] = [rid]
        else:
            self.create_index(column)
            self.count_rid[column] = 1
            col[value] = [rid]

    """ 
    # Delete Record
    """
    def delete(self, column, value, rid):
        #if value == '/':
        #    value = -MAXINT
        col = self.indices[column]
        if col.has_key(value):
            if rid in col[value]:
                col[value].remove(rid)
            if col[value] == []:
                #self.indices[column].__delitem__(value)
                col[value] = None
        return True

    """ 
    # Update record with this
    """
    def update(self, column, old_value, new_value, rid):
        #if old_value == None:
        #    old_value = -MAXINT
        #if new_value == None:
        #    new_value = -MAXINT
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)
