select count(*) as cnt,
       sum(e.record.long) as sum,
       avg(e.record.long) as avg,
       min(id2) as min,
       array_collect(e.record.long) as collect
from empty e
