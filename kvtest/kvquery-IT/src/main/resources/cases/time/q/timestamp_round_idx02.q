select id
from roundtest t
where exists t.doc.arr[timestamp_round($element, 'year') =any '2022-01-01']

