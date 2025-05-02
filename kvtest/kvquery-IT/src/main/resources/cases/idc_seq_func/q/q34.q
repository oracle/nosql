#Test: Expression using $value and < operator

select id1, 
       seq_sum(p.json.MSD.values($value < 100)) as sum
from playerinfo p