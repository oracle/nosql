select f.record.long + f.record.int
from Foo f
group by f.record.long
