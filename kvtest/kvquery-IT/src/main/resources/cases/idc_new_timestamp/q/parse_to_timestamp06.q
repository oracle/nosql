#TestCase for maps

select
t.doc.map,
parse_to_timestamp(t.doc.map,"yyyy-MM-dd'T'HH:mm:ss.SSSSSS") as parse_to_timestamp_map
from roundFunc t where id=0