#Test: seq_max() with Invalid input type. 

select id1, 
       seq_max(p.stats3.runs) as stats3_runs_max
from playerinfo p
