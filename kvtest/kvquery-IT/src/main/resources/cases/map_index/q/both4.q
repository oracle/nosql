#
# Partial key and range and always true pred
#
select id
from Foo t
where t.rec.a = 10 and
      t.rec.c.keys() >any "c1" and t.rec.c.keys() >=any "c1" and
      t.rec.c.values().ca =any 1
