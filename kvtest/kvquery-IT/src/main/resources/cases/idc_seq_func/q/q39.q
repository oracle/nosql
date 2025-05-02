#Test: seq_avg() having expr with values() as argument.

select id1, 
       seq_avg(p.stats1.Odi.Values()) as stats1_Odi_avg
from playerinfo p
