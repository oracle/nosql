select id, timestamp_ceil(t0) as t0_to_day
from roundFunc
where timestamp_ceil(t0) >= '2021-07-01T00:00:00'

