select id
from Foo t
where t.rec.c.c1.ca in (10, 1) and t.rec.c.c1.ca < 3
