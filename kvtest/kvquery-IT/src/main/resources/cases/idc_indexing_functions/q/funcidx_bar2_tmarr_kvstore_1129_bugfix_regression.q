select /*+ FORCE_INDEX(bar2 idx_year_month) */ id, tm, age, year(b.tmarr[0]) as year0, month(b.tmarr[0]) as month0, year(b.tmarr[1]) as year1, month(b.tmarr[1]) as month1, year(b.tmarr[2]) as year2, month(b.tmarr[2]) as month2
from bar2 b
where month(b.tmarr[0]) = 2 or month(b.tmarr[1]) = 2 or month(b.tmarr[2]) = 2
