#
# partial key and range, plus always-true preds
#
declare
$ext1_1 integer;
$ext1_2 integer;

select id1, id2, id4
from Foo
where id2 < 42 and id1 = $ext1_1 and id1 > 0 and id2 < $ext1_2 and id4 > "id4"
