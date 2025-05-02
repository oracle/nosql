#Test: Expression using seq_max with jnull args

select id1, 
       seq_max(jnull) as max
from playerinfo p