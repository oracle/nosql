select id, to_last_day_of_month(t.doc.s6)
from roundFunc t
where to_last_day_of_month(t.doc.s6) = '2021-11-30'