select t.Bdou, 
       case 
       when sum(t.AREC.ADOU) >= 450007 and sum(t.AREC.ADOU) < 450008 then 450007
       else sum(t.AREC.ADOU)
       end as sum
from T1 t 
group by t.BDOU
