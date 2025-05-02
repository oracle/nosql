#Test seq_max(): Expression using bin data type.

select id1, 
       seq_max(p.fbin) 
from playerinfo p