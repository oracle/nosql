#Test: seq_avg() with item returning a long.

select id1, 
       seq_avg(p.json.stats.values()) as stats_long_avg
from playerinfo p
