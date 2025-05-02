select id
from roundFunc t
where exists t.doc.arr[timestamp_round($element,'iyear') >= '2022-01-01T00:00:00']