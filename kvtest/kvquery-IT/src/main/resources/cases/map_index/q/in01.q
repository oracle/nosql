select id
from Foo t
where t.rec.a in (-10, 0) and
      t.rec.c.keys() =any "c1" and
      t.rec.c.values().ca =any 10 and
      t.rec.f = 4.5
