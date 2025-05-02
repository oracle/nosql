declare $tm2 string; // "2019-10-26T10:46:53.010123456"

select /*+ FORCE_INDEX(bar idx_hour_min_sec_ms_micro_ns) */ id
from bar b
where hour(b.tm) = hour(CAST($tm2 AS TIMESTAMP(9))) and minute(b.tm) = minute(CAST($tm2 AS TIMESTAMP(9))) and second(b.tm) = second(CAST($tm2 AS TIMESTAMP(9))) and millisecond(b.tm) = millisecond(CAST($tm2 AS TIMESTAMP(9))) and nanosecond(b.tm) = nanosecond(CAST($tm2 AS TIMESTAMP(9)))
