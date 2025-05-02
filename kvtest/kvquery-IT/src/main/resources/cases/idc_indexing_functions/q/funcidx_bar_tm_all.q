declare $tm1 string; // "2021-05-03T12:47:23.999888777"
select /*+ FORCE_INDEX(bar idx_year_month_day) */ id, year(tm), month(tm), day(tm), hour(tm), minute(tm), second(tm), millisecond(tm), microsecond(tm), nanosecond(tm)
from bar b
where year(b.tm) = year(CAST($tm1 AS TIMESTAMP(9))) and month(b.tm) = month(CAST($tm1 AS TIMESTAMP(9))) and day(b.tm) = day(CAST($tm1 AS TIMESTAMP(9))) and hour(b.tm) = hour(CAST($tm1 AS TIMESTAMP(9))) and minute(b.tm) = minute(CAST($tm1 AS TIMESTAMP(9))) and second(b.tm) = second(CAST($tm1 AS TIMESTAMP(9))) and millisecond(b.tm) = millisecond(CAST($tm1 AS TIMESTAMP(9))) and nanosecond(b.tm) = nanosecond(CAST($tm1 AS TIMESTAMP(9)))
