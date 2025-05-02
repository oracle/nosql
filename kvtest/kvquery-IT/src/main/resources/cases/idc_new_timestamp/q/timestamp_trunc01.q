#TestCase for timestamp or unit is null

select
timestamp_trunc(t0) as timestamp_trunc_null,
timestamp_trunc(t3,t0) as timestamp_trunc_time_null
from roundFunc where id=1