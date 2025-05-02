select f.record.long, sum(f.record.int) as sum
from Foo f
group by f.record.long
limit 2
