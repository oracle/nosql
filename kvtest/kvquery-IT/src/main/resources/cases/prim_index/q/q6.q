#
# equality via two range preds
# with covering index
#
select id1 + 1 as id1, id2, id4
from Foo
where id2 <= 30 and id1 = 3 and id4 = "id4-3" and 30 <= id2
