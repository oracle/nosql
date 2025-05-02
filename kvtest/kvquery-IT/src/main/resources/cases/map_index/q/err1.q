select id, t.rec.c.keys()
from Foo t
where t.rec.a = 10 and t.rec.c.keys() =any "c1"
