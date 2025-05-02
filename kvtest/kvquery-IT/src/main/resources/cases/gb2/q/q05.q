select t.BLON, 
       avg(t.AREC.BMAP.values($value != 0.0)),
       sum(t.AREC.BMAP.values($value != 0.0)), 
       count(*) 
from T1 t
where t.ID > 10023  
group by t.BLON 
limit 10
offset 2
