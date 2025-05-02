#Test: Expression using seq_maxwith * and group by operator without using index.

select id1, 
       seq_max(p.json.stats.runs * p.json.stats.matches) as max
from playerinfo p
group by p.json.stats.runs, p.json.stats.matches