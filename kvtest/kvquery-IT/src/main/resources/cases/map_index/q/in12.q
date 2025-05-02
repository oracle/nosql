select id
from Foo t
where t.rec.a in (-10, 0) and
      t.rec.c.c1.ca in (1, 5)
