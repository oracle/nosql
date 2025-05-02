select id
from Foo t
where t.rec.a = 10 and t.rec.c.c1.ca in (1, 5)
