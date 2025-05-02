#
# looks like a complete key, but we cannot actually push more than one
# multi-key column pred, even if the preds are on different multi-key
# columns.
#
select id
from Foo t
where t.rec.d[1:2].d2 =any 15 and t.rec.d[].d3 =any -6 and t.rec.f = 4.5
