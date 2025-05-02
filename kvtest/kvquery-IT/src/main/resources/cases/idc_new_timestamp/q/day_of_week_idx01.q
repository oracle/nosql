select id
from roundFunc t
where exists t.doc.arr[day_of_week($element) =any 5]