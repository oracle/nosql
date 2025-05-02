#TestCase for timestamp or unit is null

select
timestamp_round(t0) as timestamp_round_null,
timestamp_round(t3,t0) as timestamp_round_time_null
from roundFunc where id=1