select id1, sum(f.record.int)
from Foo f
group by id1
