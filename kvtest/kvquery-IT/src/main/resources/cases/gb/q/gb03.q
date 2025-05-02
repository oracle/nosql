select f.record.long, 
       sum(f.record.int * f.record.double) + sum(f.record.double),
       count(*)
from Foo f
group by f.record.long
