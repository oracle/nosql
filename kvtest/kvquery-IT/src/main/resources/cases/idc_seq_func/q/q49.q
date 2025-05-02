#Test seq_avg(): Expression using all the arith oprtrs

select id1, 
       seq_avg(p.id+p.id1*p.century-p.age/p.id) as arithOprtrs
from playerinfo p