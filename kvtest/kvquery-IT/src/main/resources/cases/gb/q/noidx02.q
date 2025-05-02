select b.xact.year, count(*)
from bar b
group by b.xact.year
