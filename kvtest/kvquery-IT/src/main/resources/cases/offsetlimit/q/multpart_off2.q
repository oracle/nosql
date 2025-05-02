#
# sort in all partitions
#
declare
$ext1 integer; // = 28

select id1, id2, id3, id4
from Foo
where id1 = 8
order by id1, id2
offset $ext1 - 18
