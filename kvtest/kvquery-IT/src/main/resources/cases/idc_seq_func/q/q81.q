#Test: seq_max() having expr with values() as args

select id1, 
       seq_max(p.stats1.T20.Values()) as stats1_T20_max
from playerinfo p
