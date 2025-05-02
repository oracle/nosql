#
# equality via two range preds
#
declare
$ext6_1 integer; //  30

select id1, id2, id4
from Foo
where id2 <= 30 and id1 = 3 and id4 = "id4-3" and $ext6_1 <= id2
