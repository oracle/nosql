select count(*) as cnt, max(c3) as max, sum(d2) as sum, avg(d2) as avg
from A.B.C.D d
where d.d2 > 200
