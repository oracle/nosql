#Test: seq_sum() with expr returning various types of data types.

select id1, 
       seq_sum(p.json.MSD.values()) as MSD_sum
from playerinfo p
