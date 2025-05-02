#Test: seq_count() with no argument.

select id1, 
       seq_count() as noargs
from playerinfo p
