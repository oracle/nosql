#Test: seq_max() having expr with values() as argument.

select id1, 
       seq_max(p.stats1.Odi.Values()) as stats1_Odi_max
from playerinfo p
