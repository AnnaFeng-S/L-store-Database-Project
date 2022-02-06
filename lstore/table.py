from lstore.index import Index
from time import time
from lstore.page import Page
from lstore.page_range import Page_Range
import math

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.directory = {}
        self.index = Index(self)
        self.page_range_list = []
        self.rid = 0
        pass    

    def __merge(self):
        print("merge is happening")
        pass
 
    def new_page_range(self):
        #print("New Page Range")
        self.page_range_list.append(Page_Range(self.num_columns, self.rid, (self.rid+16*512)))
        self.rid += 80*512
        pass

    def new_tail_page(self, page_range):
        self.page_range_list[page_range].more_tail_page(self.rid)
        self.rid += 64*512

