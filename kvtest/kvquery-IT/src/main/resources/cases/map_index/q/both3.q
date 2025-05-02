#
# Partial key and always true pred
#
select /*+ FORCE_INDEX(Foo idx_a_c_c_f) */ id
from Foo t
where t.rec.a = 10 and
      t.rec.c.keys() =any "c1" and t.rec.c.keys() >=any "c1" and
      t.rec.c.values().ca =any 1
