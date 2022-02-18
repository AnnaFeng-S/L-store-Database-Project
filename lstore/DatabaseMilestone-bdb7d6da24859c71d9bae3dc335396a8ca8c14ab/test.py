from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)

print(grades_table.page_range_list[0].b_read(0, 418))



