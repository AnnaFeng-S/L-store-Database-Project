
class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records == 512:
            return False
        else:
            return True

    def write(self, value):
        self.num_records += 1
        self.data = self.data[ : (self.num_records-1) * 8] + value.to_bytes(8, byteorder='big', signed=True) + self.data[self.num_records * 8 : ]

    def read(self, index):
        return int.from_bytes(self.data[index * 8: index * 8 + 8], byteorder='big', signed=True)

    def update(self, index, value):
        self.data = self.data[ : index * 8] + value.to_bytes(8, byteorder='big', signed=True) + self.data[index * 8 + 8 : ]
