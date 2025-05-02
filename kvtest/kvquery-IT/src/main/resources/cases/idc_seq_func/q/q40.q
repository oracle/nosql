#Test: seq_avg() having expr with values() as args

select id1, 
       seq_avg(p.stats1.T20.Values()) as stats1_T20_avg
from playerinfo p
