select f.record.int, sum(f.record.long)
from Foo f
where id1 = 1
group by f.record.int
limit 4
offset 2
