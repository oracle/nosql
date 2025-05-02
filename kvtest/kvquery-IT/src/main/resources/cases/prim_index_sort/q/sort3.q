# sort in single partition
#
declare 
$id1 integer; // 0
$id3 integer; // 3
$id4 string; // "id4"

select id1, id2, id3, id4
from Foo
where id1 = $id1 and id2 = 1 and id3 = $id3 and id4 > $id4
order by id1, id2, id3, id4
