select f.record.long, count(count(*))
from Foo f
group by f.record.long
