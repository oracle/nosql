select t.AJSO.ALON, 
       max(t.AREC.BRRY[$element.field != 0]) 
from T1 t  
where t.ID > 10023  
group by t.AJSO.ALON
limit 10 
offset 2
