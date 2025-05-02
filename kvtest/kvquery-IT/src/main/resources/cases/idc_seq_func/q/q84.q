#Test: seq_max() with expr returning various data types.

select id1, 
       seq_max(p.json.MSD.values()) as MSD_max
from playerinfo p
