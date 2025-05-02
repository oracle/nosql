select f.record.long, 
       sum(f.record.int * f.record.double) + sum(f.record.double) as sum,
       count(*) as cnt
from Foo f
group by f.record.long
order by sum(f.record.int * f.record.double)
