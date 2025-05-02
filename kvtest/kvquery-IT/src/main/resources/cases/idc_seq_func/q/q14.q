#Test: seq_count() with atomic argument.

select id1, 
       seq_count(p.age) as atomic_cnt
from playerinfo p