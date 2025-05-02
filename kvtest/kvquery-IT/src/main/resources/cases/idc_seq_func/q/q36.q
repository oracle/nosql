#Test: Expression using seq_sum(seq_sum)

select id1, 
       seq_sum(seq_sum(p.age)) as sum
from playerinfo p