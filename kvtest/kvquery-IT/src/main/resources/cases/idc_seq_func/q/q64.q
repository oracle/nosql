#Test: Expression using seq_min with more then one args

select id1, 
       seq_min(p.age,p.century) as min
from playerinfo p