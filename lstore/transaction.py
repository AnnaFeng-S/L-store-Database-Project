from lstore.table import Table, Record
from lstore.index import Index
from lstore.log import Log
import threading
import os
import pickle
SPECIAL_RID = (2 ** 64) - 1

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = []
        self.table_list = []
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
        for i in range(len(self.queries)):
            query, args = self.queries[i]
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(i)
        return self.commit()

    def abort(self,index):
        print('abort')
        index_list = []
        for i in range(len(self.table[0].log.Xact_id)):
            if threading.currentThread().ident == self.table[0].log.Xact_id[i]:
                index_list.append(i)
        for i in range(index):
            query, args = self.queries[index-i]
            temp_table = self.table[self.table_list[index-i]]
            log = temp_table.log
            temp_index = index_list[len(index_list)-1-i]
            [Page_Range, Page, Row] = log.method_information[temp_index]
            method_meta = log.method_meta[temp_index]
            old_value = log.old_value[temp_index]
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
                    temp_page_range.b_delete(Page, Row)
                    temp_table.directory.pop(ori_rid)
                    temp_page_range.base_page[Page].meta_data.num_records -= 1
                    for k in range(len(args[0])):
                        value = args[0][k]
                        if value is not None:
                            temp_table.index.delete(k,value,ori_rid)
                else:
                    #delete
                    #[rid,page,row]
                    temp_page_range.base_page[Page].meta_data.update_RID(Row, method_meta[0][0])
                    temp_table.directory[method_meta[0][0]] = [Page_Range, Page, Row]
                    for k in range(1,len(method_meta)):
                        [rid, page, row] = method_meta[k]
                        temp_page_range.tail_page[page].meta_data.update_RID(row,rid)
                    for k in range(temp_table.num_columns):
                        if temp_table.index.indices[k] is not None:
                            temp_table.index.insert(k, temp_page_range.base_page[Page].read(Row,k), rid)
            elif len(args) == 2:
                #update
                for k in range(temp_table.num_columns):
                    if temp_table.index.indices[k] is not None:
                        if args[1][k] is not None:
                            self.table.index.update(k, args[1][k], old_value[k], method_meta[0][0])
                if len(method_meta) == 2:
                    [brid, bpage, brow] = method_meta[0]
                    [trid, tpage, trow] = method_meta[1]
                    temp_page_range.base_page[bpage].meta_data.write_INDIRECTION(brid)
                    temp_page_range.tail_page[tpage].meta_data.update_RID(trow,SPECIAL_RID)
                elif len(method_meta[temp_index]) > 2:
                    [brid, bpage, brow] = method_meta[0]
                    [trid1, tpage1, trow1] = method_meta[-1]
                    [trid2, tpage2, trow2] = method_meta[-2]
                    temp_page_range.tail_page[tpage1].meta_data.update_RID(trow1, SPECIAL_RID)
                    temp_page_range.tail_page[tpage2].meta_data.write_INDIRECTION(brid)
        #index = index_list[-1-i]
        log = self.table[0].log
        log.log_num -= 1
        log.Xact_id = [log.Xact_id[i] for i in range(len(log.Xact_id)) if i not in index_list]
        log.method = [log.method[i] for i in range(len(log.method)) if i not in index_list]
        log.table_name = [log.table_name[i] for i in range(len(log.table_name)) if i not in index_list]
        log.method_information = [log.method_information[i] for i in range(len(log.method_information)) if i not in index_list]
        log.method_meta = [log.method_meta[i] for i in range(len(log.method_meta)) if i not in index_list]
        log.old_value = [log.old_value[i] for i in range(len(log.old_value)) if i not in index_list]
        return False

    def commit(self):
        if len(self.queries) == 0:
            return True
        self.table[0].lock.acquire()
        index_list = []
        for i in range(len(self.table[0].log.Xact_id)):
            if threading.currentThread().ident == self.table[0].log.Xact_id[i]:
                index_list.append(i)
        
        for i in range(len(self.queries)):
            query, args = self.queries[i]
            temp_table = self.table[self.table_list[i]]
            log = temp_table.log
            temp_index = index_list[i]
            [Page_Range, Page, Row] = log.method_information[temp_index]
            method_meta = log.method_meta[temp_index]
            if log.method[temp_index] == 0:
                #insert
                if [temp_table.name,Page_Range] in temp_table.bufferpool.bufferpool_list:
                    index = temp_table.bufferpool.bufferpool_list.index([temp_table.name,Page_Range])
                    f = open(temp_table.bufferpool.bufferpool_list[index][0] + "_" + str(temp_table.bufferpool.bufferpool_list[index][1]) + "_base_"+ str(Page) + "_meta.pickle", "wb")
                    pickle.dump(temp_table.bufferpool.bufferpool[index].base_page[Page].meta_data, f)
                    f.close()
                    for col_index in range(len(temp_table.bufferpool.bufferpool[index].base_page[Page].physical_page)):
                        with open(temp_table.bufferpool.bufferpool_list[index][0]+"_"+str(temp_table.bufferpool.bufferpool_list[index][1])+"_basepage_"+str(Page)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                            binary_file.write(temp_table.bufferpool.bufferpool[index].base_page[Page].physical_page[col_index].data)
            elif log.method[temp_index] == 1:
                #delete
                if [temp_table.name,Page_Range] in temp_table.bufferpool.bufferpool_list:
                    index = temp_table.bufferpool.bufferpool_list.index([temp_table.name,Page_Range])
                    f = open(temp_table.bufferpool.bufferpool_list[index][0] + "_" + str(temp_table.bufferpool.bufferpool_list[index][1]) + "_base_"+ str(Page) + "_meta.pickle", "wb")
                    pickle.dump(temp_table.bufferpool.bufferpool[index].base_page[Page].meta_data, f)
                    f.close()
                    for col_index in range(len(temp_table.bufferpool.bufferpool[index].base_page[Page].physical_page)):
                        with open(temp_table.bufferpool.bufferpool_list[index][0]+"_"+str(temp_table.bufferpool.bufferpool_list[index][1])+"_basepage_"+str(Page)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                            binary_file.write(temp_table.bufferpool.bufferpool[index].base_page[Page].physical_page[col_index].data)
                    for i in range(len(method_meta)):
                        Page = method_meta[i][1]
                        f = open(temp_table.bufferpool.bufferpool_list[index][0] + "_" + str(temp_table.bufferpool.bufferpool_list[index][1]) + "_tail_"+ str(Page) + "_meta.pickle", "wb")
                        pickle.dump(temp_table.bufferpool.bufferpool[index].tail_page[Page].meta_data, f)
                        f.close()
                        for col_index in range(len(temp_table.bufferpool.bufferpool[index].tail_page[Page].physical_page)):
                            with open(temp_table.bufferpool.bufferpool_list[index][0]+"_"+str(temp_table.bufferpool.bufferpool_list[index][1])+"_tailpage_"+str(Page)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                                binary_file.write(temp_table.bufferpool.bufferpool[index].tail_page[Page].physical_page[col_index].data)
            elif log.method[temp_index] == 2:
                #update
                if [temp_table.name,Page_Range] in temp_table.bufferpool.bufferpool_list:
                    index = temp_table.bufferpool.bufferpool_list.index([temp_table.name,Page_Range])
                    Page = method_meta[-1][1]
                    f = open(temp_table.bufferpool.bufferpool_list[index][0] + "_" + str(temp_table.bufferpool.bufferpool_list[index][1]) + "_tail_"+ str(Page) + "_meta.pickle", "wb")
                    pickle.dump(temp_table.bufferpool.bufferpool[index].tail_page[Page].meta_data, f)
                    f.close()
                    for col_index in range(len(temp_table.bufferpool.bufferpool[index].tail_page[Page].physical_page)):
                        with open(temp_table.bufferpool.bufferpool_list[index][0]+"_"+str(temp_table.bufferpool.bufferpool_list[index][1])+"_tailpage_"+str(Page)+"_col_"+str(col_index)+".txt", "wb") as binary_file:
                            binary_file.write(temp_table.bufferpool.bufferpool[index].tail_page[Page].physical_page[col_index].data)

        log = self.table[0].log
        log.log_num -= 1
        log.Xact_id = [log.Xact_id[i] for i in range(len(log.Xact_id)) if i not in index_list]
        log.method = [log.method[i] for i in range(len(log.method)) if i not in index_list]
        log.table_name = [log.table_name[i] for i in range(len(log.table_name)) if i not in index_list]
        log.method_information = [log.method_information[i] for i in range(len(log.method_information)) if i not in index_list]
        log.method_meta = [log.method_meta[i] for i in range(len(log.method_meta)) if i not in index_list]
        log.old_value = [log.old_value[i] for i in range(len(log.old_value)) if i not in index_list]
        self.table[0].lock.release()
        return True