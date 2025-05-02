select id1, t.rec.c[0]
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and t.rec.f = 4.5
