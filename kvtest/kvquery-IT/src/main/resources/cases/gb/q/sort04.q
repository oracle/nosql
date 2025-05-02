select sum(f.record.int), f.record.long
from Foo f
group by f.record.long
order by sum(f.record.int) * count(*)
