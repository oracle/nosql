#Test seq_min(): Expression using all the arith oprtrs on atomic.

select id1, 
       seq_min(p.id+p.id1*p.century-p.age/p.id) as arithOprtrs
from playerinfo p