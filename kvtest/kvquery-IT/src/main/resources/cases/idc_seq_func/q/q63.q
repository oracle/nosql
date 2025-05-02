#Test: Expression using seq_min(seq_min) on atomic.

select id1, 
       seq_min(seq_min(p.age)) as min
from playerinfo p