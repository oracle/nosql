select count(*) as cnt, sum(c3) as sum
from A.B.C.D d 
group by d.ida
