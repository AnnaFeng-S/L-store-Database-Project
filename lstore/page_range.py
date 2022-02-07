from lstore.bt_page import Base_Page, Tail_Page
import time

SPECIAL_RID = -(2**63)

class Page_Range:
    def __init__(self, num_columns, brid, trid):
        self.base_page = []
        self.tail_page = []
        self.n_columns = num_columns + 4
        self.next_bpage = 0
        self.next_tpage = 0
        self.next_brid = brid
        self.next_trid = trid
        self.trid_list = [trid]

    def has_capacity(self):
        if len(self.base_page) == 16 and not(self.base_page[15].has_capacity()):
            return False
        else:
            return True
    
    def tail_has_capacity(self):
        if (self.next_tpage != 0) and (len(self.tail_page) % 64 == 0) and not(self.tail_page[len(self.tail_page)-1].has_capacity()):
            return False
        else:
            return True

    def new_base_page(self):
        if len(self.base_page) < 16:
            self.base_page.append(Base_Page(self.n_columns))
            self.next_bpage += 1
        else:
            return False
    
    def new_tail_page(self):
        self.tail_page.append(Tail_Page(self.n_columns))
        self.next_tpage += 1
        
    def more_tail_page(self, new_trid):
        self.next_trid = new_trid
        self.trid_list.append(new_trid)

    def getNextRID(self):
        self.next_brid += 1
        return (self.next_brid - 1)

    def getNextTRID(self):
        self.next_trid += 1
        return (self.next_trid - 1)
        
    def b_write(self, value):
        if (self.next_brid%512 == 0):
            self.new_base_page()
        record = []
        record.append(self.getNextRID())
        record.append(0)
        record.append(record[0])
        record.append(int(time.time()))
        record = record + value
        self.base_page[self.next_bpage-1].write(record)
        loc_info = [record[0], self.next_bpage - 1, (int(self.next_brid-1) % 512)]
        return loc_info

    def t_locate(self, trid):
        page_block = 0
        for i in range(0, len(self.trid_list)):
            if trid - self.trid_list[i] < 64*512:
                page_block = i
                break
        page_index = int((trid-self.trid_list[page_block])/512) + 64*page_block
        index = int((trid-self.trid_list[page_block])%512)
        return [page_index, index]

    def b_read(self, page_index, index):
        record = []
        if self.b_read_col(page_index, index, 1) == 0:
            for i in range(4, self.n_columns):
                record.append(self.b_read_col(page_index, index, i))
        else:
            new_loc = self.b_read_col(page_index, index, 2)
            [new_page_index, new_index] = self.t_locate(new_loc)
            for i in range(4, self.n_columns):
                record.append(self.t_read_col(new_page_index, new_index, i))
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
        if ((self.next_trid-self.trid_list[len(self.trid_list)-1])%512 == 0):
            self.new_tail_page()
        new_record = []
        new_record.append(self.getNextTRID())
        new_record.append(0)
        if(self.b_read_col(page_index, index, 1) == 0):
            new_record.append(self.b_read_col(page_index, index, 0))
            new_record.append(int(time.time()))
            for i in range(4, self.n_columns):
                if values[i-4] == None:
                    new_record.append(self.b_read_col(page_index, index, i))
                else:
                    new_record.append(values[i-4])
        else:
            new_record.append(self.b_read_col(page_index, index, 2))
            new_record.append(int(time.time()))
            new_rid = self.b_read_col(page_index, index, 2)
            [new_page_index, new_index] = self.t_locate(new_rid)
            for i in range(4, self.n_columns):
                if values[i-4] == None:
                    new_record.append(self.t_read_col(new_page_index, new_index, i))
                else:
                    new_record.append(values[i-4])
        self.tail_page[self.next_tpage-1].write(new_record)
        self.b_update(page_index, index, 1, 1)
        self.b_update(page_index, index, 2, new_record[0])
        
    def b_delete(self, page_index, index):
        ori_rid = self.b_read_col(page_index, index, 0)
        self.b_update(page_index, index, 0, SPECIAL_RID)
        indirection = self.b_read_col(page_index, index, 2)
        while indirection != ori_rid:
            [new_page_index, new_index] = self.t_locate(indirection)
            self.t_update_col(new_page_index, new_index, 0, SPECIAL_RID)
            indirection = self.t_read_col(new_page_index, new_index, 2)
