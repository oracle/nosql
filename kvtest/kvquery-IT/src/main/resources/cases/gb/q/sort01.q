select f.record.long, sum(f.record.int)
from Foo f
group by f.record.long
order by sum(f.record.int)
