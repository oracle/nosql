#Test: seq_count() with "" as args.

select id1, 
       seq_count("") as cnt
from playerinfo p