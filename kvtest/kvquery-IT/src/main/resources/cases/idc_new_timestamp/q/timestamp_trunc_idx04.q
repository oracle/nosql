select id
from roundFunc t
where exists t.doc.map.keys(timestamp_trunc($key,'day') =any '2021-11-26T00:00:00')