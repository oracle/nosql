select f.record.long, 
       avg(f.record.int) + 1 as avg,
       count(*)
from Foo f
group by f.record.long
order by avg(f.record.int)
