#TestCase for arrays

select
t.doc.arr,
format_timestamp(t.doc.arr) as format_timestamp_arr
from roundFunc t where id=0