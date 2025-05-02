#Test: seq_max() with Invalid input type for the seq_maxfunction.

select id1, 
       seq_max(p.stats2.last5intest) as stats2_last5intest_max
from playerinfo p
