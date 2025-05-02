select 1 + f.record.long as c1, count(*) as cnt
from Foo f
group by f.record.long + f.record.int
