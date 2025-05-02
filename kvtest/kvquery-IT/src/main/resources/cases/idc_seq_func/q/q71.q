#Test: Expression using seq_min(seq_min) on map.

select id1, 
       seq_min(seq_min(p.json.stats.values())) as min
from playerinfo p