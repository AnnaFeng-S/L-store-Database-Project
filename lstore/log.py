class Log:
    def __init__(self):
        self.thread_id = []
        self.method = {}
        self.table_name = {}
        #[page_range, page, row]
        self.method_information = {}
        #[RID,TID,...]
        self.method_meta = {}
        self.old_value = {}