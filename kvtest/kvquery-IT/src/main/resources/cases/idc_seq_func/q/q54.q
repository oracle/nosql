#Test: seq_min() with Invalid input type for the seq_minfunction.

select id1, 
       seq_min(p.stats2.last5intest) as stats2_last5intest_min
from playerinfo p
