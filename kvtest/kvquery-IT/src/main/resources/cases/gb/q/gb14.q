select sum(f.record.int)
from Foo f
group by f.record.long
