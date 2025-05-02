#Test: Expression using seq_maxwith "" args

select id1, 
       seq_max("") as max
from playerinfo p