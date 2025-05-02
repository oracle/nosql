#Test: Result set with more than one item should not be promoted to type AnyJsonAtomic

select id1, 
       seq_sum(p.json.stats.values() * 4) as stats_long_sum
from playerinfo p
