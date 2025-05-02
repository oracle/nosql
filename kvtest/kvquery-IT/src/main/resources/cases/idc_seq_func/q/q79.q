#Test: seq_max() having expr with .Values() as argument.

select id1, 
       seq_max(p.stats1.Tests.Values()) as stats1_Tests_max
from playerinfo p
