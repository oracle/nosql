select distinct f.record.long, f.record.int
from foo f
where id1 = 0
order by f.record.int + f.record.long, f.record.int
