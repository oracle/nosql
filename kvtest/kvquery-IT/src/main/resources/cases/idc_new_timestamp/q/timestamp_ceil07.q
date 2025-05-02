#TestCase for arrays

select
t.doc.arr,
timestamp_ceil(t.doc.arr) as timestamp_ceil_arr,
timestamp_ceil(t.doc.arr,"month") as timestamp_ceil_arr_month
from roundFunc t where id=0