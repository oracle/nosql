#TestCase for timestamp or unit is null

select
timestamp_ceil(t0) as timestamp_ceil_null,
timestamp_ceil(t3,t0) as timestamp_ceil_time_null
from roundFunc where id=1