#Test: seq_count() with more then one argument.

select id1, 
       seq_count(age,name) as cnt
from playerinfo p