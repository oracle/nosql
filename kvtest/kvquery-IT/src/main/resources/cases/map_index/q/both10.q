select id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 3 and t.rec.c.c1.cd < 0 and t.rec.c.c1.cd > -100
