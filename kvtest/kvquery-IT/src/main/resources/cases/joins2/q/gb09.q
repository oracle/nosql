select ida, idb, count(*) as cnt, sum(c3) as sum, avg(c3) as avg
from A.B.C.D 
group by ida, idb
