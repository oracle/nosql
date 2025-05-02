#Test: Expression having couple of seq_sum() funcs
select id1, 
       seq_sum(p.age) + seq_sum(p.century)
from playerinfo p
