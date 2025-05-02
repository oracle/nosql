#Test: seq_concat() with one argument.

select id1, 
       seq_concat(p.stats1) as PlayerStats1
from playerinfo p
order by id
