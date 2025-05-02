#Test: seq_count() with no args.

select id1, 
       seq_count() as cnt
from playerinfo p