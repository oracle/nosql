#Test: Expression using seq_max(seq_max) on map.

select id1, 
       seq_max(seq_max(p.json.stats.values())) as max
from playerinfo p