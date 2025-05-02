select /*+ FORCE_INDEX(bar2 idx_year_month) */id
from bar2 b
where year(b.tmarr[]) =any 2021
