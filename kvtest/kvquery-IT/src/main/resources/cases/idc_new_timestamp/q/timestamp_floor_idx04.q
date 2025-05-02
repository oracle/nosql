select id
from roundFunc t
where exists t.doc.map.keys(timestamp_floor($key,'minute') =any '2021-11-26T21:50:00')