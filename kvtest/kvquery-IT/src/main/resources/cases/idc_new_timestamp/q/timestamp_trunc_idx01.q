select id, timestamp_trunc(t3) as t3_to_day
from roundFunc
where timestamp_trunc(t3) >= '2021-01-01T00:00:00'

