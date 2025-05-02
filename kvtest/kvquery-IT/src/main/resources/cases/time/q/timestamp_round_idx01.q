select id
from roundtest t
where exists t.doc.arr[timestamp_round($element) =any '2021-11-27']

