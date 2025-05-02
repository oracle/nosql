#Test: seq_sum() having expr with values() as args

select id1, 
       case when seq_sum(p.stats1.T20.Values()) >= 1234567 then 1234567
            when seq_sum(p.stats1.T20.Values()) > 19677 then 19677
            when seq_sum(p.stats1.T20.Values()) > 19547 then 19547
            when seq_sum(p.stats1.T20.Values()) > 19436 then 19436
            else seq_sum(p.stats1.T20.Values()) 
       end as stats1_T20_sum
from playerinfo p
