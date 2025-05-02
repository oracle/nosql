#TestCase for maps

select
t.doc.map,
to_last_day_of_month(t.doc.map) as to_last_day_of_month_map
from roundFunc t where id=0