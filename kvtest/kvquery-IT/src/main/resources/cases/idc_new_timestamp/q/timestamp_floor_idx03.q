select id
from roundFunc t
where exists t.doc.arr[timestamp_floor($element,'month') =any '2021-11-01T00:00:00']