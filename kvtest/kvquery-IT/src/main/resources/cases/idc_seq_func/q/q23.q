#Test: seq_sum() having expr with values() as argument.

select id1, 
       case when seq_sum(p.stats1.Tests.Values()) > 129527 then 129527
            when seq_sum(p.stats1.Tests.Values()) > 74682 then 74682
            when seq_sum(p.stats1.Tests.Values()) > 19776 then 19776
            else seq_sum(p.stats1.Tests.Values()) 
       end as stats1_Tests_sum
from playerinfo p
