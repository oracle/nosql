#Test: Expression using seq_min(seq_max) on atomic.

select id1, 
       seq_max(seq_max(p.age)) as max
from playerinfo p