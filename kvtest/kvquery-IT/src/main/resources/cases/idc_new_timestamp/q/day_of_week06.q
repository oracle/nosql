#TestCase for maps

select
t.doc.map,
day_of_week(t.doc.map) as day_of_week_map
from roundFunc t where id=0