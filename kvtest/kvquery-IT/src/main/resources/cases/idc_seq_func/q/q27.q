#Test: seq_sum() with Invalid input type for the seq_sumfunction as args.

select id1, 
       seq_sum(p.stats3.runs) as stats3_runs_sum
from playerinfo p
