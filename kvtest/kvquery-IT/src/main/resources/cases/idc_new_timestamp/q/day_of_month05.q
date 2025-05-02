#TestCase for arrays

select
t.doc.arr,
day_of_month(t.doc.arr) as day_of_month_arr
from roundFunc t where id=0