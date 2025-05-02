select id
from roundFunc t
where exists t.doc.arr[timestamp_ceil($element,'year') =any '2021-01-01T00:00:00']