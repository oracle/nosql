select bdou, count(bdou) as cnt, 
       case 
       when sum(bdou) >= 450007 and sum(bdou) < 450008 then 450007
       else sum(bdou)
       end as sum
from t1 
group by bdou
