#TestCase for arrays

select
t.doc.arr,
to_last_day_of_month(t.doc.arr) as to_last_day_of_month_arr
from roundFunc t where id=0