select t.ALON, t.AREC.ADOU, sum(t.ADOU + t.ALON) 
from T1 t  
group by t.ALON, t.AREC.ADOU
