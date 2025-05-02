#TestCase for keys

select
t.doc.map,
timestamp_floor(t.doc.map) as timestamp_floor_map,
timestamp_floor(t.doc.map,"month") as timestamp_floor_map_month
from roundFunc t where id=0