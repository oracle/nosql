select /*+ FORCE_INDEX(bar2 idx_year_month) */ id, tm, age
from bar2 b
where year(b.tmarr[1]) =2020 and month(b.tmarr[1]) =11
