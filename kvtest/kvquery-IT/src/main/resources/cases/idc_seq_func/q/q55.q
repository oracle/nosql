#Test: seq_min() with Invalid input type. 

select id1, 
       seq_min(p.stats3.runs) as stats3_runs_min
from playerinfo p
