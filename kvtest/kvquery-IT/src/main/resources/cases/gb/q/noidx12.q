select f.record.int, sum(f.record.long)
from Foo f
group by f.record.int
limit 2
offset 2
