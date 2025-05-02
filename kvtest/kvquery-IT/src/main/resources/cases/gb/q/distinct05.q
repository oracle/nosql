select distinct f.record.long, f.record.int
from foo f
where id1 = 1
order by f.record.int + f.record.long, f.record.int
