#TestCase for arrays

select
t.doc.arr,
timestamp_floor(t.doc.arr) as timestamp_floor_arr,
timestamp_floor(t.doc.arr,"month") as timestamp_floor_arr_month
from roundFunc t where id=0