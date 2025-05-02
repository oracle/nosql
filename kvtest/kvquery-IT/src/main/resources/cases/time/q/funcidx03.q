select /* FORCE_INDEX(bar2 idx_year_month) */id
from bar2 b
where exists b.tmarr[year($element) = 2021 and month($element) < 6]
