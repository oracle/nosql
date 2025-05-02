#Test: Expression using seq_min with "" args

select id1, 
       seq_min("") as min
from playerinfo p