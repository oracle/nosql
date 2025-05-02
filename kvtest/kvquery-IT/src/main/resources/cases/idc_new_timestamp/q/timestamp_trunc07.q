#TestCase for arrays

select
t.doc.arr,
timestamp_trunc(t.doc.arr) as timestamp_trunc_arr,
timestamp_trunc(t.doc.arr,"month") as timestamp_trunc_arr_month
from roundFunc t where id=0