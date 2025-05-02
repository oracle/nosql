#Test: seq_min() having expr with .Values() as argument.

select id1, 
       seq_min(p.stats1.Tests.Values()) as stats1_Tests_min
from playerinfo p
