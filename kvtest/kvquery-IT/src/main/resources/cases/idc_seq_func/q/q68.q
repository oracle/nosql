#Test: Expression using seq_min with jnull args

select id1, 
       seq_min(jnull) as min
from playerinfo p