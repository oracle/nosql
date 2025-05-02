select id, t.record
from foo t
where t.record.long < 10 and
      (t.record.int is null or t.record.int = -20)
