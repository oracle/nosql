#
# Partial key. Looks like complete key but isn't
#
select id
from Foo t
where t.rec.a = 10 and
      t.rec.c.keys() =any "c1" and
      t.rec.c.values().ca =any 10 and
      t.rec.f = 4.5
