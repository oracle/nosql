
select id, g
from Foo t
where t.rec.a = 10 and t.rec.c.c1.ca = 3 and t.rec.c.keys() =any "c1"
