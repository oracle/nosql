select id, timestamp_ceil(t0, 'month') as t0_to_month
from roundtest
where timestamp_ceil(t0, 'month') = '2021-12-01'

