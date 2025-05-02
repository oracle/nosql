select count(d2) as cnt, sum(d2) as sum, avg(d2) as avg
from A.B.C.D d
where d.d2 > 20 and d.c3 < 40
