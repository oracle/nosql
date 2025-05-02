declare $tm1 string; // "2021-05-03T12:47:23.999888777"

select /*+ FORCE_INDEX(bar idx_year_month_day) */ id
from bar b
where month(b.tm, CAST($tm1 AS TIMESTAMP(9))) = 5
