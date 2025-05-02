select id
from roundtest t
where year(timestamp_trunc(t.s9, 'day')) = 2021

