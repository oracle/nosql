select t.AINT, t.ALON, t.AFLO, t.ADOU, t.ANUM, avg(t.ANUM) 
from T1 t  
where t.ID > 10023  
group by t.AINT, t.ALON, t.AFLO, t.ADOU, t.ANUM
limit 10
offset 2
