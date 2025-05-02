select sum(f.record.long)
from Foo f
group by f.record.int
