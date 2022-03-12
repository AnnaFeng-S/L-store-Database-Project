from lstore.bt_page import Base_Page, Tail_Page
from lstore.page_range_meta import Page_Range_Meta
import time

SPECIAL_RID = (2 ** 64) - 1
RID_COLUMN = 0
BASE_PAGE_MAX_SIZE = 16
PHYSICAL_PAGE_SIZE = 512


class Page_Range:
    def __init__(self, num_columns, brid, trid, tail_block_size):
        self.base_page = []
        self.tail_page = []
        self.meta = Page_Range_Meta(num_columns, brid, trid, tail_block_size)
        self.used_time = 0
        self.dirty = 0
        self.pin = 0
        #self.lock_manager = lock_manager

    def has_capacity(self):
        if len(self.base_page) == BASE_PAGE_MAX_SIZE and not (self.base_page[15].has_capacity()):
            return False
        else:
            return True

    def tail_has_capacity(self):
        if (self.meta.next_tpage != 0) and (len(self.tail_page) % self.meta.tail_block_size == 0) and not (
        self.tail_page[len(self.tail_page) - 1].has_capacity()):
            return False
        else:
            return True

    def new_base_page(self):
        if len(self.base_page) < BASE_PAGE_MAX_SIZE:
            self.base_page.append(Base_Page(self.meta.n_columns))
            self.meta.next_bpage += 1
        else:
            return False

    def new_tail_page(self):
        self.tail_page.append(Tail_Page(self.meta.n_columns))
        self.meta.next_tpage += 1

    def more_tail_page(self, new_trid):
        self.meta.next_trid = new_trid
        self.meta.trid_list.append(new_trid)

    def getNextRID(self):
        self.meta.next_brid += 1
        return (self.meta.next_brid - 1)

    def getNextTRID(self):
        self.meta.next_trid += 1
        return (self.meta.next_trid - 1)

    def b_write(self, values):
        if (self.meta.next_brid % PHYSICAL_PAGE_SIZE == 0):
            self.new_base_page()
        rid = self.getNextRID()
        self.base_page[-1].meta_data.write_RID(rid)
        self.base_page[-1].meta_data.write_INDIRECTION(rid)
        self.base_page[-1].meta_data.write_TIMESTAMP(int(time.time()))
        for i in range(0, self.meta.n_columns):
            self.base_page[-1].write_col(i, values[i])
        loc_info = [rid, self.meta.next_bpage - 1, rid % PHYSICAL_PAGE_SIZE]
        return loc_info

    def t_locate(self, trid):
        page_block = 0
        for i in range(0, len(self.meta.trid_list)):
            if trid - self.meta.trid_list[i] < self.meta.tail_block_size * PHYSICAL_PAGE_SIZE:
                page_block = i
                break
        page_index = int((trid - self.meta.trid_list[page_block]) / PHYSICAL_PAGE_SIZE) + self.meta.tail_block_size * page_block
        index = int((trid - self.meta.trid_list[page_block]) % PHYSICAL_PAGE_SIZE)
        return [page_index, index]

    def b_read_index_col(self, page_index, index, i):
        indirection = self.base_page[page_index].meta_data.read_INDIRECTION(index)
        tps = self.base_page[page_index].meta_data.read_TPS()
        record = None
        if self.base_page[page_index].meta_data.read_RID(index) == indirection or indirection < tps:
            record = self.b_read_col(page_index, index, i)
        else:
            new_loc = self.base_page[page_index].meta_data.read_INDIRECTION(index)
            [new_page_index, new_index] = self.t_locate(new_loc)
            if self.base_page[page_index].meta_data.read_bit(i, index) == 1:
                record = self.t_read_col(new_page_index, new_index, i)
            else:
                record = self.b_read_col(page_index, index, i)
        return record

    def b_read(self, page_index, index):
        record = []
        indirection = self.base_page[page_index].meta_data.read_INDIRECTION(index)
        tps = self.base_page[page_index].meta_data.read_TPS()
        if self.base_page[page_index].meta_data.read_RID(index) == indirection or indirection < tps:
            for i in range(0, self.meta.n_columns):
                record.append(self.b_read_col(page_index, index, i))
        else:
            new_loc = self.base_page[page_index].meta_data.read_INDIRECTION(index)
            [new_page_index, new_index] = self.t_locate(new_loc)
            for i in range(0, self.meta.n_columns):
                if self.base_page[page_index].meta_data.read_bit(i, index) == 1:
                    record.append(self.t_read_col(new_page_index, new_index, i))
                else:
                    record.append(self.b_read_col(page_index, index, i))
        return record

    def b_update(self, page_index, index, column, value):
        self.base_page[page_index].update(index, column, value)

    def t_update_col(self, page_index, index, column, value):
        self.tail_page[page_index].update(index, column, value)

    def b_read_col(self, page_index, index, column):
        return self.base_page[page_index].read(index, column)

    def t_read_col(self, page_index, index, column):
        return self.tail_page[page_index].read(index, column)

    def t_update(self, page_index, index, values):
        if ((self.meta.next_trid - self.meta.trid_list[-1]) % PHYSICAL_PAGE_SIZE == 0):
            self.new_tail_page()
        base_rid = self.base_page[page_index].meta_data.read_RID(index)
        indirection = self.base_page[page_index].meta_data.read_INDIRECTION(index)
        tps = self.base_page[page_index].meta_data.read_TPS()
        next_tid = self.getNextTRID()
        self.tail_page[-1].meta_data.write_TID(next_tid)
        self.tail_page[-1].meta_data.write_RID(base_rid)
        self.tail_page[-1].meta_data.write_TIMESTAMP(int(time.time()))
        if base_rid == indirection:
            self.tail_page[-1].meta_data.write_INDIRECTION(base_rid)
            for i in range(0, self.meta.n_columns):
                if values[i] != None:
                    self.tail_page[-1].write(i, values[i])
                    self.base_page[page_index].meta_data.set_bit(i, index)
                else:
                    self.tail_page[-1].increment(i)
        else:
            self.tail_page[-1].meta_data.write_INDIRECTION(indirection)
            if indirection < tps:
                for i in range(0, self.meta.n_columns):
                    if values[i] != None:
                        self.tail_page[-1].write(i, values[i])
                        self.base_page[page_index].meta_data.set_bit(i, index)
                    else:
                        if self.base_page[page_index].meta_data.read_bit(i, index) == 1:
                            self.tail_page[-1].write(i, self.b_read_col(page_index, index, i))
                        else:
                            self.tail_page[-1].increment(i)
            else:
                [new_page_index, new_index] = self.t_locate(indirection)
                for i in range(0, self.meta.n_columns):
                    if values[i] != None:
                        self.tail_page[-1].write(i, values[i])
                        self.base_page[page_index].meta_data.set_bit(i, index)
                    else:
                        if self.base_page[page_index].meta_data.read_bit(i, index) == 1:
                            self.tail_page[-1].write(i, self.t_read_col(new_page_index, new_index, i))
                        else:
                            self.tail_page[-1].increment(i)
        self.base_page[page_index].meta_data.update_INDIRECTION(index, next_tid)

    def b_delete(self, page_index, index):
        ori_rid = self.base_page[page_index].meta_data.read_RID(index)
        self.base_page[page_index].meta_data.update_RID(index, SPECIAL_RID)
        indirection = self.base_page[page_index].meta_data.read_INDIRECTION(index)
        while indirection != ori_rid:
            [new_page_index, new_index] = self.t_locate(indirection)
            self.tail_page[new_page_index].meta_data.dirty = 1
            self.tail_page[new_page_index].meta_data.update_TID(new_index, SPECIAL_RID)
            indirection = self.tail_page[new_page_index].meta_data.read_INDIRECTION(new_index)

    def merge(self):
        for base_page_index in range(0, len(self.base_page) - 1):
            self.base_page_merge(base_page_index)
        if (self.base_page[-1].has_capacity()):
            return len(self.base_page) - 1
        else:
            self.base_page_merge(len(self.base_page) - 1)
            return len(self.base_page)

    def base_page_merge(self, base_page_index):
        num_records = self.base_page[base_page_index].meta_data.read_num_record()
        self.base_page[base_page_index].meta_data.create_LUTIME()
        for index in range(0, num_records):
            rid = self.base_page[base_page_index].meta_data.read_RID(index)
            indirection = self.base_page[base_page_index].meta_data.read_INDIRECTION(index)
            tps = self.base_page[base_page_index].meta_data.read_TPS()
            if rid == SPECIAL_RID or indirection < tps:
                continue
            if (rid != indirection):
                [new_page_index, new_index] = self.t_locate(indirection)
                for column_index in range(0, self.meta.n_columns):
                    condition = self.base_page[base_page_index].meta_data.read_bit(column_index, index)
                    if condition == 1:
                        temp_data = self.t_read_col(new_page_index, new_index, column_index)
                        self.b_update(base_page_index, index, column_index, temp_data)
                self.base_page[base_page_index].meta_data.update_LUTIME(index, int(time.time()))
        base_tid = self.base_page[base_page_index].meta_data.merge_time
        self.base_page[base_page_index].meta_data.update_TPS(base_tid + (self.meta.tail_block_size * PHYSICAL_PAGE_SIZE))
