from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
import os
import sys

db = Database()
db.open("./Interface")
table_list = {}
query_list = {}
os.system("clear")
print("Welcome to the LStore Interface")

batch_mode = 0

if len(sys.argv) == 3:
    batch_mode = 1
    file_name = sys.argv[2]
    print("Batch Mode: " + file_name)
    path = "../" + file_name
    read_file = open(path, "r")

while(1):
    if batch_mode == 1:
        user_input = read_file.readline()
        if user_input == "":
            break
        else:
            user_input = user_input.strip()
            print("> " + user_input)
    else:
        user_input = input("> ")

    user_input = user_input.upper()
    if user_input == "EXIT":
        break
    elif "INSERT" in user_input:
        # INSERT(1, 2, 3, 4, 5) INTO table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        query = query_list[table_name]
        table = table_list[table_name]
        for i in range(0, len(user_input)):
            user_input[i] = int(user_input[i])
        query.insert(*user_input)
    elif "UPDATE" in user_input:
        # UPDATE(key, [None, None, 1, 3, None]) INTO table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        key = int(user_input[0])
        update_cols = []
        user_input[1] = user_input[1][1:]
        user_input[len(user_input) - 1] = user_input[len(user_input) - 1][:-1]

        for i in range(1, table_list[table_name].num_columns+1):
            if user_input[i] == "NONE":
                update_cols.append(None)
            else:
                update_cols.append(int(user_input[i]))
        query = query_list[table_name]
        query.update(key, *update_cols)

    elif "SELECT" in user_input:
        # SELECT(key, 0, [1, 1, 1, 1, 1]) FROM TABLE_NAME
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        key = int(user_input[0])
        col = int(user_input[1])
        select_cols = []
        user_input[2] = user_input[2][1:]
        user_input[len(user_input) - 1] = user_input[len(user_input) - 1][:-1]
        for i in range(2, len(user_input)):
            select_cols.append(int(user_input[i]))
        query = query_list[table_name]
        records = query.select(key, col , select_cols)
        for record in records:
            if record != None:
                print(record.columns)
    elif "SUM" in user_input:
        # SUM(0, 100, 0) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        start_range = int(user_input[0])
        end_range = int(user_input[1])
        aggregate_column_index = int(user_input[2])
        query = query_list[table_name]
        print(query.sum(start_range, end_range, aggregate_column_index))
    elif "DELETE" in user_input:
        # DELETE(key) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        key = int(user_input)
        query = query_list[table_name]
        query.delete(key)
    elif "CREATE TABLE" in user_input:
        # CREATE TABLE (grades, 5, 0)
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        table_name = user_input[0]
        num_columns = int(user_input[1])
        main_key = int(user_input[2])
        table_list[table_name] = db.create_table(table_name, num_columns, main_key)
        for i in range(1, num_columns):
            table_list[table_name].index.create_index(i)
        query_list[table_name] = Query(table_list[table_name])
    elif "MIN" in user_input:
        # MIN(0,100,0) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        start_range = int(user_input[0])
        end_range = int(user_input[1])
        min_column_index = int(user_input[2])
        query = query_list[table_name]
        print(query.min(start_range, end_range, min_column_index))
    elif "MAX" in user_input:
        # MAX(0,100,0) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        start_range = int(user_input[0])
        end_range = int(user_input[1])
        max_column_index = int(user_input[2])
        query = query_list[table_name]
        print(query.max(start_range, end_range, max_column_index))
    elif "AVERAGE" in user_input:
        # AVERAGE(0,100,0) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        start_range = int(user_input[0])
        end_range = int(user_input[1])
        average_column_index = int(user_input[2])
        query = query_list[table_name]
        print(query.average(start_range, end_range, average_column_index))
    elif "COUNT" in user_input:
        # COUNT(INDEX, COLUMN) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        user_input = user_input.split(",")
        index = int(user_input[0])
        column = int(user_input[1])
        query = query_list[table_name]
        print(query.count(index, column))
    elif "FIRST" in user_input:
        # FIRST(COLUMN) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        column = int(user_input)
        query = query_list[table_name]
        print(query.first(column))
    elif "LAST" in user_input:
        # LAST(COLUMN) FROM table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        user_input = user_input[0]
        user_input = user_input.split("(")[1].split(")")[0]
        column = int(user_input)
        query = query_list[table_name]
        print(query.last(column))
    elif "GET TABLE" in user_input:
        # GET TABLE table_name
        user_input = user_input.split(" ")
        table_name = user_input[2]
        table = db.get_table(table_name)
        if table == None:
            print("Table does not exist")
        else:
            table_list[table_name] = table
            query_list[table_name] = Query(table)
    elif "CLEAR" in user_input:
        os.system("clear")
        print("Welcome to the LStore Interface")
    
db.close()
