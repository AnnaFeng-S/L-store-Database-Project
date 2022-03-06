from lstore.table import Table, Record
from lstore.index import Index
from lstore.log import Log
import pickle
import os
SPECIAL_RID = (2 ** 64) - 1

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = []
        self.table_list = []
        self.current_thread_id = 0
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if table not in self.table:
            self.table.append(table)
        self.table_list.append(self.table.index(table))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for i in len(self.queries):
            query, args = self.queries[i]
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(i)
        return self.commit()

    def abort(self,index):
        index_list = []
        for i in range(len(self.table[0].log.Xact_id)):
            if self.current_thread_id == self.table[0].log.Xact_id[i]:
                index_list.append(i)
        for i in range(index):
            query, args = self.queries[index-i]
            temp_table = self.table[self.table_list[index-i]]
            log = temp_table.log
            temp_index = index_list[len(index_list)-1-i]
            [Page_Range, Page, Row] = log.method_information[temp_index]
            method_meta = log.method_meta[temp_index]
            if [temp_table.name,Page_Range] in temp_table.bufferpool.bufferpool_list:
                temp_page_range = temp_table.bufferpool.bufferpool[
                    temp_table.bufferpool.bufferpool_list.index([temp_table.name, Page_Range])]
            elif temp_table.bufferpool.has_capacity() == True:
                temp_table.bufferpool.bufferpool_list.append([temp_table.name, Page_Range])
                temp_page_range = temp_table.bufferpool.disk_to_memory(temp_table.name, Page_Range)
                temp_table.bufferpool.bufferpool.append(temp_page_range)
            else:
                temp_index = temp_table.bufferpool.min_used_time()
                temp_table.bufferpool.memory_to_disk(temp_index)
                temp_page_range = temp_table.bufferpool.disk_to_memory(temp_table.name, Page_Range)
                temp_table.bufferpool.bufferpool_list[temp_index] = [temp_table.name, Page_Range]
                temp_table.bufferpool.bufferpool[temp_index] = temp_page_range
            #method: insert：0，delete:1,update:2,sum/select:3
            if len(args) == 1:
                if type(args[0]) == list:
                    #insert
                    ori_rid = temp_page_range.base_page[Page].meta_data.read_RID(Row)
                    temp_page_range.base_page[Page].meta_data.update_RID(Row,SPECIAL_RID)
                    temp_table.directory.pop(ori_rid)
                else:
                    #delete
                    #[rid,page,row]
                    temp_page_range.base_page[Page].meta_data.update_RID(Row, method_meta[0][0])
                    temp_table.directory[method_meta[0][0]] = [Page_Range, Page, Row]
                    for k in range(1,len(method_meta)):
                        [rid, page, row] = method_meta(k)
                        temp_page_range.tail_page[page].meta_data.update_RID(row,rid)       
            elif len(args) == 2:
                #update
                pass
            else:
                #sum, select
                pass
            return False

    def commit(self):
        index_list = []
        for i in range(len(self.table[0].log.Xact_id)):
            if self.current_thread_id == self.table[0].log.Xact_id[i]:
                index_list.append(i)
        for i in range(len(self.queries)):
            query, args = self.queries[i]
            temp_table = self.table[self.table_list[i]]
            log = temp_table.log
            temp_index = index_list[i]
            [Page_Range, Page, Row] = log.method_information[temp_index]
            method_meta = log.method_meta[temp_index]
            if len(args) == 0:
                if [temp_table.name,Page_Range] in temp_table.bufferpool.bufferpool_list:
                    f = open(self.bufferpool_list[Page_Range][0] + "_" + str(self.bufferpool_list[Page_Range][1]) + "_base_"+ str(Page) + "_meta.pickle", "wb")
                    pickle.dump(self.bufferpool[Page_Range].base_page[Page].meta_data, f)
                    os.path.join(self.path,self.bufferpool_list[Page_Range][0] + "_" + str(self.bufferpool_list[Page_Range][1]) + "_base_"+ str(Page) + "_meta.pickle")
                    f.close()
                    for col_index in range(len(self.bufferpool[Page_Range].base_page[Page].physical_page)):
                        with open(self.bufferpool_list[Page_Range][0]+"_"+str(self.bufferpool_list[Page_Range][1])+"_basepage_"+str(Page)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                            binary_file.write(self.bufferpool[Page_Range].base_page[Page].physical_page[col_index].data)
                            os.path.join(self.path, self.bufferpool_list[Page_Range][0]+"_"+str(self.bufferpool_list[Page_Range][1])+"_basepage_"+str(Page)+"_col_"+str(col_index)+".txt")
            elif len(args) == 2 or 1:
                t_index = method_meta[1]
                f = open(self.bufferpool_list[Page_Range][0] + "_" + str(self.bufferpool_list[Page_Range][1]) + "_tail_"+ str(t_index) + "_meta.pickle", "wb")
                pickle.dump(self.bufferpool[Page_Range].tail_page[t_index].meta_data, f)
                os.path.join(self.path,self.bufferpool_list[Page_Range][0] + "_" + str(self.bufferpool_list[Page_Range][1]) + "_tail_"+ str(t_index) + "_meta.pickle")
                f.close()
                for col_index in range(len(self.bufferpool[Page_Range].tail_page[t_index].physical_page)):
                    with open(self.bufferpool_list[Page_Range][0]+"_"+str(self.bufferpool_list[Page_Range][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                        binary_file.write(self.bufferpool[Page_Range].tail_page[t_index].physical_page[col_index].data)
                        os.path.join(self.path, self.bufferpool_list[Page_Range][0]+"_"+str(self.bufferpool_list[Page_Range][1])+"_tailpage_"+str(t_index)+"_col_"+str(col_index)+".txt")
        return True

