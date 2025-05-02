#
# sort in all partitions
#
select id1, id2, id3, id4
from Foo
order by id1, id2
limit 15 offset 3
