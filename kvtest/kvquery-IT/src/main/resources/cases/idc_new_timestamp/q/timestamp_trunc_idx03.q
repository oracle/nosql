select id
from roundFunc t
where exists t.doc.arr[timestamp_trunc($element,'second') =any '2021-11-26T21:50:30']