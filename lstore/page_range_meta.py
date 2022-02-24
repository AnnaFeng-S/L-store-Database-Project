class Page_Range_Meta:
    def __init__(self, num_columns, brid, trid, tail_block_size):
        self.n_columns = num_columns
        self.next_bpage = 0
        self.next_tpage = 0
        self.next_brid = brid
        self.next_trid = trid
        self.trid_list = [trid]
        self.tail_block_size = tail_block_size
        self.Last_saved_base = 0
        self.Last_saved_tail = 0