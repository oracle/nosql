select id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 3 and t.rec.c.c1.cb < 15 and
      t.rec.c.c2.cc = 105 and t.rec.c.c2.cd = -5 and t.rec.c.c2.cb = 11
