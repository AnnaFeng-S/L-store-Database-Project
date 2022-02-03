from lstore.page import Page

class Base_Page:
    def __init__(self, num_columns):
        self.base_page = []
        for i in range (0, num_columns):
            self.base_page.append(Page())
    
    def has_capacity(self):
        return self.base_page[0].has_capacity()

    # write a list of values to the page
    def write(self, values):
        for i in range (0, len(self.base_page)):
            self.base_page[i].write(values[i])

    def read(self, index, column):
        return self.base_page[column].read(index)

    def update(self, index, column, value):
        self.base_page[column].update(index, value)

class Tail_Page:
    def __init__(self, num_columns):
        self.tail_page = []
        for i in range (0, num_columns):
            self.tail_page.append(Page())
    
    def has_capacity(self):
        return self.tail_page[0].has_capacity()

    def write(self, values):
        for i in range (0, len(self.tail_page)):
            self.tail_page[i].write(values[i])

    def read(self, index, column):
        return self.tail_page[column].read(index)

    def update(self, index, column, value):
        self.tail_page[column].update(index, value)
