#Test: seq_max() with items returning int,long,number

select id1, 
       seq_max(p.json.stats.values()) as stats_max
from playerinfo p
