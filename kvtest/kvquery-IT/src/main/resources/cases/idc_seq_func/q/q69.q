#Test: Expression using seq_min with * and group by operator withut using index.

select id1, 
       seq_min(p.json.stats.runs * p.json.stats.matches) as min
from playerinfo p
group by p.json.stats.runs, p.json.stats.matches