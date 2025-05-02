#Test seq_max(): Expression using array slice step

select id1, 
       seq_max(p.json.stats2[2:4]) as maxElement
from playerinfo p