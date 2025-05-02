#Test: seq_sum() having expr with values() as argument.

select id1, 
       case when seq_sum(p.stats1.Odi.Values()) > 159504 then 159504
            when seq_sum(p.stats1.Odi.Values()) > 130167 then 130167
            when seq_sum(p.stats1.Odi.Values()) > 120539 then 120539
            else seq_sum(p.stats1.Odi.Values()) 
       end as stats1_Odi_sum
from playerinfo p
