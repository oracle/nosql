select f.record.long, sum(f.record.int)
from Foo f
group by f.record.long
limit 4
offset 6
