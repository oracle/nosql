select /*+ FORCE_INDEX(bar idx_year_month_day) */ id
from bar b
where day(null) = 3
