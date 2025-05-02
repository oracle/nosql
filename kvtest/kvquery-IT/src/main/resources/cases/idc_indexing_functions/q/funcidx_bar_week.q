declare $week1 string; // "2021-02-03"

select /*+ FORCE_INDEX(bar idx_week) */ id, week(b.tm) as week
from bar b
where week(b.tm) = week(CAST($week1 AS TIMESTAMP(0)))
