#Test: Expression using seq_min with null args

select id1, 
       seq_min(null) as min
from playerinfo p