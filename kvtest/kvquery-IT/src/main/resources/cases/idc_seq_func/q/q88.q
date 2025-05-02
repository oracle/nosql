#Test seq_max: Expression having couple of seq_max() functions on atomic.
select id1, 
       seq_max(p.age) + seq_max(p.century)
from playerinfo p
