#TestCase for maps

select
t.doc.map,
format_timestamp(t.doc.map) as format_timestamp_map
from roundFunc t where id=0