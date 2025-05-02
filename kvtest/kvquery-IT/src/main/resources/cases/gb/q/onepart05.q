select id1, id2, sum(f.record.int) as sum, count(*) as cnt
from Foo f
where id1 = 1
group by id1, id2
