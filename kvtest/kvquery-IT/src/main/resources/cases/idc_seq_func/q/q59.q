#Test seq_min: Result set with more than one item should not be promoted to type AnyJsonAtomic

select id1, 
       seq_min(p.json.stats.values() * 4) as min
from playerinfo p
