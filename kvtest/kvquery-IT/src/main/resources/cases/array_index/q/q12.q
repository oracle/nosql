#
# partial key
#
select id,  [ t.rec.d.d2 ]
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and (t.rec.f = 4.5 or t.rec.d[0].d2 > 0) 
