select /*+ FORCE_INDEX(bar idx_hour_min_sec_ms_micro_ns) */ id
from bar b
where nanosecond() = 999888777
