select id
from Foo t
where t.g = 5 and exists t.rec.c.values($value.ca in (10, 6))
