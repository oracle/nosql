#Test seq_max(): Expression using array filter step

select id1, 
       seq_max(p.json.stats2[$element > 100 and $pos < 10]) as maxElement
from playerinfo p