#Test: seq_concat() with null as argument.

select id1, 
       seq_concat(null) as nullAsArgs
from playerinfo p
order by id
