select id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 3 and t.rec.c.c1.cb = 10 and
      t.rec.c.c2.cc = 101 and t.rec.c.c2.cd = -101
