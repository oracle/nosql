#Test: seq_avg() with Invalid input type for the seq_avg function.

select id1, 
       seq_avg(p.stats2.last5intest) as stats2_last5intest_avg
from playerinfo p
