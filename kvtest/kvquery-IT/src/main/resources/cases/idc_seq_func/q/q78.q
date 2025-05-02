#Test seq_min(): Expression using array filter step

select id1, 
       seq_min(p.json.stats2[$element > 100 and $pos < 10]) 
from playerinfo p