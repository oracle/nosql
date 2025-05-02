#Test: Expression using all the arith oprtrs

select id1, 
       seq_sum(p.id+p.id1*p.century-p.age/p.id) as arithOprtrs
from playerinfo p