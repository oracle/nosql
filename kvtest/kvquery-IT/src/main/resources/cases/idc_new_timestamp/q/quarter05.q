#TestCase for arrays

select
t.doc.arr,
quarter(t.doc.arr) as quarter_arr
from roundFunc t where id=0