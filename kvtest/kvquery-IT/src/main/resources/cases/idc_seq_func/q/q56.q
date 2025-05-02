#Test: seq_min() with expr returning various data types.

select id1, 
       seq_min(p.json.MSD.values()) as MSD_min
from playerinfo p
