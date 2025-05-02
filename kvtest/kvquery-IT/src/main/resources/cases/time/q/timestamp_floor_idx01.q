select id
from roundtest t
where timestamp_floor(t.doc.s6, 'year') = '2021-01-01'

