
select sum(f.record.int), f.record.long
from Foo f
group by f.record.long
