#Test seq_max(): Expression using Invalid input type for the seq_maxfunction:Map(RECORD)

select id1, 
       seq_max(p.stats1) 
from playerinfo p