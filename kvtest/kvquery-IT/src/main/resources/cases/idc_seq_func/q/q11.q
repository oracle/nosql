#Test: seq_count() with json column as argument.

select id1, 
       seq_count(p.json) as json_count
from playerinfo p