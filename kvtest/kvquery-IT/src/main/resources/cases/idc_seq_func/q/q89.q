#Test seq_max: Expression using $value and < operator

select id1, 
       seq_max(p.json.MSD.values($value < 100)) as max
from playerinfo p