select id, timestamp_floor(t3) as t3_to_day
from roundFunc
where timestamp_floor(t3) >= '2020-01-01T00:00:00'

