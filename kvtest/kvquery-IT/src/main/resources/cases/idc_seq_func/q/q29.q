#Test: seq_sum() with item returning a long.

select id1, 
       seq_sum(p.json.stats.values()) as stats_sum
from playerinfo p
