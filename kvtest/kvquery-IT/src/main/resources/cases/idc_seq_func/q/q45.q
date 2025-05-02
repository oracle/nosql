#Test: seq_avg() if there is at least one input item of type number, the result will be a number.
select id1, 
       seq_avg(p.json.stats.values()) as stats_avg
from playerinfo p
