#Test: Expression using seq_min with no args

select id1, 
       seq_min() as min
from playerinfo p