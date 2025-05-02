select id
from roundtest t
where month(timestamp_floor(t.doc.s6)) = 11

