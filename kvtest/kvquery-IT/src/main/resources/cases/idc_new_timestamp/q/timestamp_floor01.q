#TestCase for timestamp or unit is null

select
timestamp_floor(t0) as timestamp_floor_null,
timestamp_floor(t3,t0) as timestamp_floor_time_null
from roundFunc where id=1