#Test: seq_count() with json column returning count as.

select id1, 
       seq_count(p.json.MSD.values()) as MSD
from playerinfo p
where id1=4
