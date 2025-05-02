#Test: seq_min() with expr using .keys()
select id1, 
       seq_min(p.json.stats.keys()) as stats_min
from playerinfo p
