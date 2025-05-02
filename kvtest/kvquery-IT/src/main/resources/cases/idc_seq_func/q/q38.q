#Test: seq_avg() having expr with values() as argument.

select id1, 
       seq_avg(p.stats1.Tests.Values()) as stats1_Tests_avg
from playerinfo p
