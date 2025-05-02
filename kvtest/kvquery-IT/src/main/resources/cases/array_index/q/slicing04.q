#
# partial key and what looks like an always true pred, but isn't.
#
select id
from Foo t
where t.rec.d[0:1].d2 <=any 15 and t.rec.d[$element.d3 = 3].d2 =any 15
