#Test: seq_concat() with more then one argument.

select id1, 
       seq_concat(p.stats1, p.stats2, p.stats3, p.json.Virat) as PlayerStats
from playerinfo p
order by id
