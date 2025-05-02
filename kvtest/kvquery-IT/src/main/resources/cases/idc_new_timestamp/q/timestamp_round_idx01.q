select id, timestamp_round(t0) as t0_to_day
from roundFunc
where timestamp_round(t0) >= '2021-01-01T00:00:00'

