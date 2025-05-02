select id1, id2, sum(f.record.int) as sum
from Foo f
where id1 = 0
group by id1, id2
limit 2

