#TestCase for arrays

select
t.doc.arr,
day_of_week(t.doc.arr) as day_of_week_arr
from roundFunc t where id=0