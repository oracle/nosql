#TestCase for arrays

select
t.doc.arr,
timestamp_round(t.doc.arr) as timestamp_round_arr,
timestamp_round(t.doc.arr,"month") as timestamp_round_arr_month
from roundFunc t where id=0