#
# partial key and range, plus always-true preds
# covering index
#
select id1, id2, id4
from Foo t
where id2 < 42 and id1 = 4 and id1 > 0 and id2 < 50 and id4 > "id4"
