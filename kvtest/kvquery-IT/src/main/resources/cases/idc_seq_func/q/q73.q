#Test seq_min(): Expression using bin data type.

select id1, 
       seq_min(p.bin) 
from playerinfo p