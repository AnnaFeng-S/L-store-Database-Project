from lstore.bt_page import Base_Page, Tail_Page

class Page_Range:
    def __init__(self, num_columns):
        self.base_page = []
        self.tail_page = []
        self.n_columns = num_columns
        self.next_bpage = 0
        self.next_tpage = 0
        self.next_brid = 0
        self.next_trid = 0

    def has_capacity(self):
        pass

    def new_base_page(self):
        if self.next_bpage <= 16:
            self.base_page.append(Base_Page(self.n_columns))
            self.next_bpage += 1
        else:
            return False
    
    def new_tail_page(self):
        
    
    def b_write(self, value):
        for i in range (0, self.n_columns):
            self.base_page[(self.next_bpage-1)].write(value[i], i)

    def b_read(self, page_index, index, column):
        return self.base_page[page_index].read(index, column)
