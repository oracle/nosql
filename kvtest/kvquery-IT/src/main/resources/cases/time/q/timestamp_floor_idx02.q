select id
from roundtest t
where timestamp_floor(t.doc.s6) = '2021-11-26'

