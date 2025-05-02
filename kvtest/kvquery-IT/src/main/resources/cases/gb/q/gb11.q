select f.record.long, 
       avg(f.record.int),
       count(*)
from Foo f
group by f.record.long
