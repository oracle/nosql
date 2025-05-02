#Test: seq_concat() with "" as argument.

select id1, 
       seq_concat("") as emptyStringAsArgs
from playerinfo p
order by id
