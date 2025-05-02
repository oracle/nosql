select f.record.long + 1, 
       sum(f.record.int * f.record.double) + sum(f.record.double)
from Foo f
group by f.record.long
order by count(f.record.long)
