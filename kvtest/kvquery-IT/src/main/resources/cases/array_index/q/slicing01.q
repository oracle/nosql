#
# partial key
#
select id
from Foo t
where t.rec.d[1:2].d2 =any 15
