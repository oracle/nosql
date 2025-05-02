#Test with all the seq functions together.

select id, 
       seq_sum(s.statsinfo.values()) as statsSum,
       seq_avg(s.statsinfo.values()) as statsAvg,
       seq_min(s.statsinfo.values()) as statsMin,
       seq_max(s.statsinfo.values()) as statsMax,
       seq_count(s.statsinfo.values()) as statsCount,
       seq_concat(s.statsinfo.values()) as statsConcat
from stats s
