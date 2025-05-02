#Test seq_min: Expression having couple of seq_min() functions on atomic.
select id1, 
       seq_min(p.age) + seq_min(p.century)
from playerinfo p
