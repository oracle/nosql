#Test: seq_count() with json argument.

select id1, 
       seq_count(p.json) as json_Keys
from playerinfo p