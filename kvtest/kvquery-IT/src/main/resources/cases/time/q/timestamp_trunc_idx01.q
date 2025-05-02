select id
from roundtest t
where timestamp_trunc(t.s9) = '2021-02-28'
