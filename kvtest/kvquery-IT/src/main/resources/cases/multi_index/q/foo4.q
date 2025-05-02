#
# d_f wins over primary with higher score (due to filtering preds)
#
select id1, [ t.rec.d.d2 ]
from Foo t
where id1 = 0 and t.rec.d.d2 =any 10
