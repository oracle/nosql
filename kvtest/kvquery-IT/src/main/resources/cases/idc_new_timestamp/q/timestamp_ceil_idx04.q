select id
from roundFunc t
where exists t.doc.map.keys(timestamp_ceil($key,'month') =any '2021-12-01')