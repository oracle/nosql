#Test seq_min(): Expression using Invalid input type for the seq_minfunction:Map(RECORD)

select id1, 
       seq_min(p.stats1) 
from playerinfo p