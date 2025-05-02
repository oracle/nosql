# partial key
select id, t.rec.b
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3
