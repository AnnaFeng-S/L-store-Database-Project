from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        print("Database created")
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        print("Table " + name + " created")
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        tables.remove(name)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
