#Test: seq_count() an expression with values() as argument.

select id1, 
       seq_count(p.stats1.Odi.values()) as stats1_Odi_Values
from playerinfo p