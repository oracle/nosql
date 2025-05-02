#Test: seq_min() with items returning int,long,number

select id1, 
       seq_min(p.json.stats.values()) as stats_min
from playerinfo p
