#
# sort in single partition
#
select id1, id2, id3, id4
from Foo
where id2 <= 3 and id1 = 3 and 3 <= id2
order by id1, id2, id3, id4
