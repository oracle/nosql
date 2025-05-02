#Test seq_max(): Expression using all the arith oprtrs on atomic.

select id1, 
       seq_max(p.id+p.id1*p.century-p.age/p.id) as arithOprtrs
from playerinfo p