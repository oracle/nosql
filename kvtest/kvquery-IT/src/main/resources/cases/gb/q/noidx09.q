select f.record.int, sum(f.record.long)
from Foo f
group by f.record.int
offset 2
