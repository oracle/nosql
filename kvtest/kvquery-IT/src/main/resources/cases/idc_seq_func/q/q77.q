#Test seq_min(): Expression using array slice step

select id1, 
       seq_min(p.json.stats2[2:4]) 
from playerinfo p