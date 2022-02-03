from lstore.index import Index
from lstore.page_range import Page_Range
from time import time

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
        self.page_directory = {}
        self.index = Index(self)
        pass

    def get_page_directory(self, rid, page_range):
        for r in rid:
            if r in page_range.base_page.brid:
                self.page_directory[r] = page_range.base_page.read()
            else:
                self.page_directory[r] = page_range.tail_page.read()
        return self.page_directory
        pass



    def __merge(self):
        print("merge is happening")
        pass
 
