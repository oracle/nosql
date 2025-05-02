select id, g
from Foo t
where t.rec.a = 10 and t.rec.c.values().ca =any 3 and t.rec.c.c1.ca in (3, 11)
