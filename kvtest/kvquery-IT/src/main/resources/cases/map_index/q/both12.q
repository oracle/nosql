select id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 3 and t.rec.c.c1.cb = 10 and
      t.rec.c.c1.cc = 100 and t.rec.c.c1.cd = -100 and
      t.rec.f = 4.5
