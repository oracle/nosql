select f.record.long, sum(f.record.int)
from Foo f
where sum(f.record.int) > 0
group by f.record.long
