#
# partial key and range, plus always-true preds
#
declare
$ext2_1 integer; // 4
$ext2_2 integer; // 41
$ext2_3 integer; // 42

select id1, id2, id4
from Foo
where id2 < $ext2_3 + $ext2_2 - 41 and id1 = $ext2_1 + 2 - 2 and
      id1 > 0 and id2 < $ext2_2 and id4 > "id4"
