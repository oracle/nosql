select /* FORCE_INDEX(bar idx_year_hour) */ hour(tm), count(*) as cnt
from bar
where year(tm) = 2021
group by hour(tm)
