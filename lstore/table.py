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
BASE_PAGE_RECORD = 16 * 512
TAIL_BLOCK_RECORD = TAIL_PAGE_BLOCK_SIZE * 512
PAGE_RANGE_RECORD = (16 + TAIL_PAGE_BLOCK_SIZE) * 512


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

    def __init__(self, name, num_columns, key, pool, bufferpool, Log):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.directory = {}
        self.rid = 0
        self.index = Index(self)
        self.page_range_list = []
        self.page_range_num = 0
        self.pool = pool
        self.lock = threading.Lock()
        self.bufferpool = bufferpool
        self.log = Log
        pass

    def __merge(self, Page_Range):
        self.lock.acquire()
        if [self.name, Page_Range] in self.bufferpool.bufferpool_list:
            temp_page_range = self.bufferpool.bufferpool[
                self.bufferpool.bufferpool_list.index([self.name, Page_Range])]
            temp_page_range.pin += 1
        elif self.bufferpool.has_capacity() == True:
            self.bufferpool.bufferpool_list.append([self.name, Page_Range])
            temp_page_range = self.bufferpool.disk_to_memory(self.name, Page_Range)
            self.bufferpool.bufferpool.append(temp_page_range)
            temp_page_range.pin += 1
        else:
            temp_index = self.bufferpool.min_used_time()
            if self.bufferpool.bufferpool[temp_index].dirty == 1:
                self.bufferpool.memory_to_disk(temp_index)
            temp_page_range = self.bufferpool.disk_to_memory(self.name, Page_Range)
            self.bufferpool.bufferpool_list[temp_index] = [self.name, Page_Range]
            self.bufferpool.bufferpool[temp_index] = temp_page_range
            temp_page_range.pin += 1
        #print("Merge Page Range: " + str(Page_Range))
        self.lock.release()
        page_range_copy = copy.deepcopy(temp_page_range)
        num_updated = page_range_copy.merge()
        self.lock.acquire()
        for i in range(num_updated):
            temp_page_range.base_page[i].physical_page = page_range_copy.base_page[i].physical_page
            temp_page_range.base_page[i].meta_data.TPS = page_range_copy.base_page[i].meta_data.TPS
            temp_page_range.base_page[i].meta_data.merge_time += 1
        temp_page_range.dirty = 1
        temp_page_range.pin -= 1
        self.lock.release()
        #print("Thread is done")

    def new_page_range(self):
        # print("New Page Range")
        self.page_range_list.append(
            Page_Range(self.num_columns, self.rid, (self.rid + BASE_PAGE_RECORD), TAIL_PAGE_BLOCK_SIZE))
        self.rid += PAGE_RANGE_RECORD
        self.page_range_num += 1
        pass

    def new_tail_page(self, page_range):
        self.page_range_list[page_range].more_tail_page(self.rid)
        self.rid += TAIL_BLOCK_RECORD
        self.pool.add_task(self.__merge, page_range)