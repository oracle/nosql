#Test: seq_max() with expr using .keys()
select id1, 
       seq_max(p.json.stats.keys()) as stats_max
from playerinfo p
