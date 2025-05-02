select count(id3) + sum(e.record.long) as sum,
       max(id2) as max
from empty e
where id2 > 5
