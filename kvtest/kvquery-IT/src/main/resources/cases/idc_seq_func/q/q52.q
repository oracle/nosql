#Test: seq_min() having expr with values() as argument.

select id1, 
       seq_min(p.stats1.Odi.Values()) as stats1_Odi_min
from playerinfo p
