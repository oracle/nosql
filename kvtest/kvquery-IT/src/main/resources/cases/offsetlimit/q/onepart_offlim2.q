#
# sort in single partition
#
declare
$ext1 integer;
select id1, id2, id3, id4
from Foo
where id1 = 8 and id2 = 4
order by id1, id2
limit 13 offset 2
