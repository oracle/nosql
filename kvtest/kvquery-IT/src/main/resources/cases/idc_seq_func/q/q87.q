#Test seq_max: Result set with more than one item should not be promoted to type AnyJsonAtomic

select id1, 
       seq_max(p.json.stats.values() * 4) as min
from playerinfo p
