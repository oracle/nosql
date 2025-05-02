#TestCase for maps

select
t.doc.map,
day_of_month(t.doc.map) as day_of_month_map
from roundFunc t where id=0