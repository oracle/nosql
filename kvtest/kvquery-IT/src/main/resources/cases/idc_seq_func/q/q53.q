#Test: seq_min() having expr with values() as args

select id1, 
       seq_min(p.stats1.T20.Values()) as stats1_T20_min
from playerinfo p
