class Base_Meta:
    def __init__(self, num_columns): 
        self.RID = bytearray(4096)
        self.SCHEMA = bytearray(4096)
        self.INDIRECTION = bytearray(4096)
        self.TIMESTAMP = bytearray(4096)
        self.num_columns = num_columns
        self.num_records = 0
        self.TPS = 0

    def has_capacity(self):
        if self.num_records == 512:
            return False
        else:
            return True

    def write_RID(self, RID):
        self.num_records += 1
        self.RID[(self.num_records-1) * 8: self.num_records * 8] = RID.to_bytes(8, byteorder='big')
    
    def write_INDIRECTION(self, INDIRECTION):
        self.INDIRECTION[(self.num_records-1) * 8: self.num_records * 8] = INDIRECTION.to_bytes(8, byteorder='big')
    
    def write_TIMESTAMP(self, TIMESTAMP):
        self.TIMESTAMP[(self.num_records-1) * 8: self.num_records * 8] = TIMESTAMP.to_bytes(8, byteorder='big')

    def read_RID(self, index):
        return int.from_bytes(self.RID[index * 8: index * 8 + 8], byteorder='big')
    
    def read_SCHEMA(self, index):
        return int.from_bytes(self.SCHEMA[index * 8: index * 8 + 8], byteorder='big')

    def read_INDIRECTION(self, index):
        return int.from_bytes(self.INDIRECTION[index * 8: index * 8 + 8], byteorder='big')
    
    def read_TIMESTAMP(self, index):
        return int.from_bytes(self.TIMESTAMP[index * 8: index * 8 + 8], byteorder='big')

    def read_TPS(self):
        return self.TPS
    
    def read_num_record(self):
        return self.num_records

    def set_bit(self, n, index):
        initial = int.from_bytes(self.SCHEMA[index * 8: index * 8 + 8], byteorder='big')
        self.SCHEMA[index * 8: index * 8 + 8] = (initial | (1 << n)).to_bytes(8, byteorder='big')

    def read_bit(self, n, index):
        initial = int.from_bytes(self.SCHEMA[index * 8: index * 8 + 8], byteorder='big')
        return (initial >> n) & 1

    def update_RID(self, index, RID):
        self.RID[index * 8: index * 8 + 8] = RID.to_bytes(8, byteorder='big')

    def update_SCHEMA(self, index, SCHEMA):
        self.SCHEMA[index * 8: index * 8 + 8] = SCHEMA.to_bytes(8, byteorder='big')
    
    def update_INDIRECTION(self, index, INDIRECTION):
        self.INDIRECTION[index * 8: index * 8 + 8] = INDIRECTION.to_bytes(8, byteorder='big')

    def update_TPS(self, new_TPS):
        self.TPS = new_TPS

class Tail_Meta:
    def __init__(self):
        self.TID = bytearray(4096)
        self.RID = bytearray(4096)
        self.INDIRECTION = bytearray(4096)
        self.TIMESTAMP = bytearray(4096)
        self.num_records = 0

    def write_TID(self, TID):
        self.num_records += 1
        self.TID[(self.num_records-1) * 8: self.num_records * 8] = TID.to_bytes(8, byteorder='big')
    
    def write_RID(self, RID):
        self.RID[(self.num_records-1) * 8: self.num_records * 8] = RID.to_bytes(8, byteorder='big')
    
    def write_INDIRECTION(self, INDIRECTION):
        self.INDIRECTION[(self.num_records-1) * 8: self.num_records * 8] = INDIRECTION.to_bytes(8, byteorder='big')
    
    def write_TIMESTAMP(self, TIMESTAMP):
        self.TIMESTAMP[(self.num_records-1) * 8: self.num_records * 8] = TIMESTAMP.to_bytes(8, byteorder='big')
    
    def read_TID(self, index):
        return int.from_bytes(self.TID[index * 8: index * 8 + 8], byteorder='big')
    
    def read_RID(self, index):
        return int.from_bytes(self.RID[index * 8: index * 8 + 8], byteorder='big')
    
    def read_INDIRECTION(self, index):
        return int.from_bytes(self.INDIRECTION[index * 8: index * 8 + 8], byteorder='big')
    
    def read_TIMESTAMP(self, index):
        return int.from_bytes(self.TIMESTAMP[index * 8: index * 8 + 8], byteorder='big')

    def read_num_record(self):
        return self.num_records
    
    def update_TID(self, index, TID):
        self.TID[index * 8: index * 8 + 8] = TID.to_bytes(8, byteorder='big')