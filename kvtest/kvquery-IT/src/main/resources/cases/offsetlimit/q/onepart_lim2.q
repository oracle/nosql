#
# sort in single partition
#
select id1, id2, id3, id4
from Foo
where id1 = 8 and id2 = 4
order by id1, id2, id3, id4
limit 0
