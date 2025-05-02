select id
from roundFunc t
where exists t.doc.map.keys(timestamp_round($key,'iweek') > '2023-05-28T00:00:00')