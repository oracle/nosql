#Test: seq_concat() with no argument.

select id1, 
       seq_concat() as NoArgs
from playerinfo p
order by id
