select f.record.long, min(f.record.int) as min, max(f.record.int) as max
from Foo f
group by f.record.long
