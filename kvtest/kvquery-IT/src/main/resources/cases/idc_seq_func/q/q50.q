#Test: Expression using seq_avg(seq_avg)

select id1, 
       seq_avg(seq_avg(p.age)) as avg
from playerinfo p