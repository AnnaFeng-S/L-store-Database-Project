from lstore.index import Index
from time import time
from lstore.page import Page
from lstore.page_range import Page_Range
from lstore.merge_thread import ThreadPool
from queue import Queue
import math
import threading
import copy
import time


RID_COLUMN = 0
SCHEMA_ENCODING_COLUMN = 1
INDIRECTION_COLUMN = 2
TIMESTAMP_COLUMN = 3
TAIL_PAGE_BLOCK_SIZE = 32
BASE_PAGE_RECORD = 16*512
TAIL_BLOCK_RECORD = TAIL_PAGE_BLOCK_SIZE*512
PAGE_RANGE_RECORD = (16+TAIL_PAGE_BLOCK_SIZE)*512


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

    def __init__(self, name, num_columns, key, pool, lock):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.directory = {}
        self.index = Index(self)
        self.page_range_list = []
        self.rid = 0
        self.pool = pool
        self.lock = lock
        pass

    def __merge(self, page_range):
        #print("Merge is happening for page range: " + str(page_range))
        page_range_copy = copy.deepcopy(self.page_range_list[page_range])
        num_updated = page_range_copy.merge()
        if self.lock.acquire():
            #print("Get the lock, others cannot access the page range")
            try:
                for i in range(num_updated):
                    self.page_range_list[page_range].base_page[i].physical_page = page_range_copy.base_page[i].physical_page
                    self.page_range_list[page_range].base_page[i].meta_data.TPS = page_range_copy.base_page[i].meta_data.TPS
                self.page_range_list[page_range].merge_time += 1
            finally:
                time.sleep(1)
                #print("Release the lock")
                self.lock.release()
                #print("Lock released")
        else:
            raise Exception("Could not acquire lock")
        #print("Updated TPS" + str(page_range_copy.base_page[0].meta_data.TPS))
        #print("Thread is done")
 
    def new_page_range(self):
        #print("New Page Range")
        self.page_range_list.append(Page_Range(self.num_columns, self.rid, (self.rid + BASE_PAGE_RECORD), TAIL_PAGE_BLOCK_SIZE))
        self.rid += PAGE_RANGE_RECORD
        pass

    def new_tail_page(self, page_range):
        self.page_range_list[page_range].more_tail_page(self.rid)
        self.rid += TAIL_BLOCK_RECORD
        self.pool.add_task(self.__merge, page_range)

