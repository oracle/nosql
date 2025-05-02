#TestCase for arrays

select
t.doc.arr,
day_of_year(t.doc.arr) as day_of_year_arr
from roundFunc t where id=0