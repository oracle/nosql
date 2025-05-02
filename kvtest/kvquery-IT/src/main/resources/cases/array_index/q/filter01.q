#
# partial key and always true pred.
#
select id
from Foo t
where t.rec.d.d2 >=any 3 and t.rec.d[$element.d3 = 3].d2 =any 15
