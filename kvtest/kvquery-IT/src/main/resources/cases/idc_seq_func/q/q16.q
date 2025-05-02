#Test: seq_count() with null as args.

select id1, 
       seq_count(null) as cnt
from playerinfo p