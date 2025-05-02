select id, timestamp_ceil(t0) as t0_to_day
from roundtest
where timestamp_ceil(t0) >= '2021-11-27'