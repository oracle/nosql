#TestCase for maps

select
t.doc.map,
day_of_year(t.doc.map) as day_of_year_map
from roundFunc t where id=0