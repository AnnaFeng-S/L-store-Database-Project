from lstore.index import Index
from time import time
from lstore.page import Page
import math

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

MAX_PAGE_RANGE = 80*512
MAX_BASE_PAGE = 16*512
MAX_PAGE = 512

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
        self.page_directory = {}
        self.index = Index(self)
        self.page_range_list = []
        pass
    """
    Return [Page_Range, Page, Row, IsBasePage]
    All begin from 0
    IsBasePage is a 1 bit boolean value
    """
    def directory(self, rid):
        Page_Range = math.floor(rid/MAX_PAGE_RANGE)
        if (rid % MAX_PAGE_RANGE)<MAX_BASE_PAGE:
            IsBasePage = 1
            Page = math.floor((rid % MAX_PAGE_RANGE)/MAX_PAGE)
        else:
            IsBasePagege = 0
            Page = math.floor((rid % MAX_PAGE_RANGE)/MAX_PAGE)-16
        Row = rid % MAX_PAGE
        return [Page_Range,Page,Row,IsBasePage]
    

    def __merge(self):
        print("merge is happening")
        pass
 
