select /*+ FORCE_INDEX(bar2 idx_starr_year) */ id, b.starr[substring($element, 0, 4) = "2018"] as elements_with_year_2018
from bar2 b
where exists b.starr[substring($element, 0, 4) = "2018"]
