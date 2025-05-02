select t.bnum, count(*), avg(t.arec.anum)
from T1 t
group by t.bnum
