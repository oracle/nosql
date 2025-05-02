#Test: seq_sum() if there is at least one input item of type number, the result will be a number
select id1, 
       seq_sum(p.json.stats.values()) as stats_sum
from playerinfo p
