declare $tm1 string; // "2021-05-03T12:47:23.999888777"

select /*+ FORCE_INDEX(bar idx_hour_min_sec_ms_micro_ns) */ id
from bar b
where nanosecond(b.tm, CAST($tm1 AS TIMESTAMP(9))) = 999888777
