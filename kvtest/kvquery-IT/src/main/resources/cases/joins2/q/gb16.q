select ida, count(*) as cnt, sum(c3) as sum, max(d2) as max
from A.B.C.D
group by ida, c3
