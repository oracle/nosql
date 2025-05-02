#Test seq_min: Expression using $value and < operator

select id1, 
       seq_min(p.json.MSD.values($value < 100)) as min
from playerinfo p