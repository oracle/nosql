#Test: Expression using seq_maxwith null args

select id1, 
       seq_max(null) as max
from playerinfo p