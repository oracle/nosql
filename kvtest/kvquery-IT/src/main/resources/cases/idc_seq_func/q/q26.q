#Test: seq_sum() with Invalid input type for the seq_sumfunction as args.

select id1, 
       seq_sum(p.stats2.last5intest) as stats2_last5intest_sum
from playerinfo p
