#
# sort in all partitions
#
select id1, id2, id3, id4
from Foo
where id1 = 8
order by id1, id2
limit 10 offset 3
