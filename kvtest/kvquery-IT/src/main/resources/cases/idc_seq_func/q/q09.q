#Test: seq_count() an expression having keys() as argument.

select id1, 
       seq_count(p.stats1.keys()) as stats1keys
from playerinfo p