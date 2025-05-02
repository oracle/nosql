select ida, idb, count(*) as cnt, sum(c3) as sum, avg(c3) as avg
from A.B.C.D 
where c3 > 0 and c3 < 100
group by ida, idb
