select d2, count(*) as cnt, sum(d2) as sum, min(c3) as max
from A.B.C.D
where ida = 40 
group by d2
