select f.record.int
from Foo f
group by f.record.int
order by count(*), f.record.int
