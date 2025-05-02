#Test: Expression using seq_max with more then one args

select id1, 
       seq_max(p.age,p.century) as max
from playerinfo p