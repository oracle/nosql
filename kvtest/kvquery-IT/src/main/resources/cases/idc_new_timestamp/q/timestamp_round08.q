#TestCase for keys

select
t.doc.map,
timestamp_round(t.doc.map) as timestamp_round_map,
timestamp_round(t.doc.map,"month") as timestamp_round_map_month
from roundFunc t where id=0