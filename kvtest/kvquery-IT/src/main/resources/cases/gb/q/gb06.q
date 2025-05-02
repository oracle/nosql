
select f.record.long + 1 as long, 
       sum(f.record.int * f.record.double) + sum(f.record.double) as sum
from Foo f
group by f.record.long
