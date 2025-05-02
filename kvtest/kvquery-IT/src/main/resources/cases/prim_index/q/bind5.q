#
# complete primary key
#
declare
$ext5_1 integer;  // 30
$ext5_2 string;   // "tok0"

select id1, id2, id4
from Foo
where id2 = $ext5_1 and id1 = 3 and id1 > 0 and id2 < 50 and
      id4 = "id4-3" and id3 = $ext5_2
