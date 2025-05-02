#TestCase for keys

select
t.doc.map,
timestamp_ceil(t.doc.map) as timestamp_ceil_map,
timestamp_ceil(t.doc.map,"month") as timestamp_ceil_map_month
from roundFunc t where id=0