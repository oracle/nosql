select a.c1, a.ida, count(*) as cnt
from nested tables (A as a descendants(A.B as b))
group by a.c1, a.ida
order by a.c1, a.ida


