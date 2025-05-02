#TestCase for keys

select
t.doc.map,
timestamp_trunc(t.doc.map) as timestamp_trunc_map,
timestamp_trunc(t.doc.map,"month") as timestamp_trunc_map_month
from roundFunc t where id=0