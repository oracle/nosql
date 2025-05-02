select id
from roundtest t
where to_last_day_of_month(t.doc.s6) = '2021-11-30' and year(l3) = 2024
