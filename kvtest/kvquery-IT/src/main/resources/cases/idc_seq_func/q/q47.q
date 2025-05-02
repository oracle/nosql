#Test seq_avg: Expression having couple of seq_avg() funcs
select id1, 
       seq_avg(p.age) + seq_avg(p.century)
from playerinfo p
