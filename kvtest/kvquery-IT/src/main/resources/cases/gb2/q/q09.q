select t.AINT, t.ALON, count(*) as cnt, sum(t.ALON) as sum
from T1 t  
group by t.AINT, t.ALON
