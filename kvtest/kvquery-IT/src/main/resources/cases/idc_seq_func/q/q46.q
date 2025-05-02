#Test seq_avg: Result set with more than one item should not be promoted to type AnyJsonAtomic

select id1, 
       seq_avg(p.json.stats.values() * 4) as stats_long_avg
from playerinfo p
