#Test: seq_avg() with Invalid input type for the seq_avg function.

select id1, 
       seq_avg(p.stats3.runs) as stats3_runs_avg
from playerinfo p
