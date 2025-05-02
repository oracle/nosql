select f.record.long, sum(f.record.str)
from Foo f
group by f.record.long
