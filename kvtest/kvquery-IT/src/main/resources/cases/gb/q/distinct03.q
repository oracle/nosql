select distinct f.record.long, f.record.int
from foo f
order by f.record.int, f.record.long
