
select id2, sum(f.record.int)
from Foo f
where id1 = 0
group by id1, id2
offset 1
