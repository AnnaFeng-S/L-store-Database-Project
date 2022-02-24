from lstore.page import Page
from lstore.meta import Base_Meta, Tail_Meta


class Base_Page:
    def __init__(self, num_columns):
        self.meta_data = Base_Meta(num_columns)
        self.physical_page = []
        for i in range(0, num_columns):
            self.physical_page.append(Page())
        self.dirty = 0

    def has_capacity(self):
        return self.meta_data.has_capacity()

    def write(self, values):
        for i in range(0, len(self.physical_page)):
            self.physical_page[i].write(values[i])

    def write_col(self, col, value):
        self.physical_page[col].write(value)

    def read(self, index, column):
        return self.physical_page[column].read(index)

    def update(self, index, column, value):
        self.physical_page[column].update(index, value)


class Tail_Page:
    def __init__(self, num_columns):
        self.physical_page = []
        self.meta_data = Tail_Meta()
        for i in range(0, num_columns):
            self.physical_page.append(Page())
        self.dirty = 0

    def has_capacity(self):
        return self.physical_page[0].has_capacity()

    def write(self, i, value):
        self.physical_page[i].write(value)

    def increment(self, i):
        self.physical_page[i].num_records += 1

    def read(self, index, column):
        return self.physical_page[column].read(index)

    def update(self, index, column, value):
        self.physical_page[column].update(index, value)
