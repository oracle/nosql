#TestCase for maps

select
t.doc.map,
quarter(t.doc.map) as quarter_map
from roundFunc t where id=0