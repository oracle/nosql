#
# partial key and range, plus always-true preds
# covering index
#
select id1, id2, id4
from Foo
where 42 > id2 and 0 < id1 and id1 = 4 and id2 < 50E0 and id4 > "id4"
