select t.AINT, t.ALON, t.AFLO, t.ADOU,
       count(*) as cnt,
       sum(t.AREC.values($value > 0.0)) as sum
from T1 t
where t.ID > 10023
group by t.AINT, t.ALON, t.AFLO, t.ADOU 
limit 10
offset 2
