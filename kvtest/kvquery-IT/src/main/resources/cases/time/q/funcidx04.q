select id, concat(year(tm), "-", month(tm))
from bar
where year(tm) = 2021 and month(tm) = 2
