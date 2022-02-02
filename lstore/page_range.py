from lstore.bt_page import Base_Page, Tail_Page
import datetime

class Page_Range:
    def __init__(self, num_columns, brid, trid):
        self.base_page = []
        self.tail_page = []
        self.n_columns = num_columns+4
        self.ini_trid = trid
        self.next_bpage = 0
        self.next_tpage = 0
        self.next_brid = brid
        self.next_trid = trid

    def has_capacity(self):
        if len(self.base_page) == 16 and not(self.base_page[15].has_capacity()):
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
        if len(self.tail_page) < 64:
            self.tail_page.append(Tail_Page(self.n_columns))
            self.next_tpage += 1
        else:
            return False

    def getNextRID(self):
        self.next_brid += 1
        return (self.next_brid - 1)

    def getNextTRID(self):
        self.next_trid += 1
        return (self.next_trid - 1)
        
    def b_write(self, value):
        if not(self.has_capacity()):
            return False
        if (self.next_brid%512 == 0):
            self.new_base_page()
        record = []
        record.append(self.getNextRID())
        record.append(0)
        record.append(record[0])
        record.append(int(round(datetime.datetime.now().timestamp())))
        record = record + value
        self.base_page[self.next_bpage-1].write(record)
        location = []
        location.append(self.next_bpage-1)
        location.append(record[0]%512)
        return location

    def b_read(self, page_index, index):
        record = []
        if self.b_read_col(page_index, index, 1) == 0:
            for i in range(0, self.n_columns):
                record.append(self.b_read_col(page_index, index, i))
        else:
            new_loc = self.b_read_col(page_index, index, 2)
            new_page_index = int((new_loc-self.ini_trid)/512)
            new_index = int((new_loc-self.ini_trid)%512)
            for i in range(0, self.n_columns):
                record.append(self.t_read_col(new_page_index, new_index, i))
        return record
    
    def b_update(self, page_index, index, column, value):
        self.base_page[page_index].update(index, column, value)

    def b_read_col(self, page_index, index, column):
        return self.base_page[page_index].read(index, column)

    def t_read_col(self, page_index, index, column):
        return self.tail_page[page_index].read(index, column)

    def t_update(self, page_index, index, column, value):
        if ((self.next_trid-self.ini_trid)%512 == 0):
            self.new_tail_page()
        new_record = []
        new_record.append(self.getNextTRID())
        new_record.append(0)
        new_index = index
        new_page_index = page_index
        if(self.b_read_col(page_index, index, 1) == 0):
            new_record.append(self.b_read_col(page_index, index, 0))
        else:
            new_record.append(self.b_read_col(page_index, index, 2))
            new_loc = self.b_read_col(page_index, index, 2)
            new_page_index = int((new_loc-self.ini_trid)/512)
            new_index = int((new_loc-self.ini_trid)%512)
        new_record.append(int(round(datetime.datetime.now().timestamp())))
        for i in range(4, self.n_columns):
            if i == column+4:
                new_record.append(value)
            else:
                if(self.b_read_col(page_index, index, 1) == 0):
                    new_record.append(self.b_read_col(page_index, index, i))
                else:
                    new_record.append(self.t_read_col(new_page_index, new_index, i))
        self.tail_page[self.next_tpage-1].write(new_record)
        self.b_update(page_index, index, 1, 1)
        self.b_update(page_index, index, 2, new_record[0])
        
