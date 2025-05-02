#Test: seq_avg() with expr returning various types of data types.

select id1, 
       seq_avg(p.json.MSD.values()) as MSD_avg
from playerinfo p
