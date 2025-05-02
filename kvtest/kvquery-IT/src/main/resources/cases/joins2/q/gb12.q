select ida, idb, count(*) as cnt, sum(c3) as sum, max(c3) as max
from A.B.C.D
where c3 > 200 
group by ida, idb
