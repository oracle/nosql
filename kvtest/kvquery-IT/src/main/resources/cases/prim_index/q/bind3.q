#
# always false
#
declare
$ext3_1 integer;  // 42

select id1, id2, id4
from Foo
where $ext3_1 > id2 and 0 < id1 and id1 = 4 and id2 > 50 and id4 > "id4"
