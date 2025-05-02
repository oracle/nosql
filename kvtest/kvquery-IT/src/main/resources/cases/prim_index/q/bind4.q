#
# always false
#
declare
$ext4_1 integer;  // 60

select id1, id2, id4
from Foo
where $ext4_1 > id2 and 0 < id1 and id1 = -4 and id2 > 50 and id4 > "id4"

