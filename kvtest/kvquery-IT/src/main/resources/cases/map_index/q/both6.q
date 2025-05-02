select /*+ FORCE_INDEX(Foo idx_a_c_c_f) */id, g
from Foo t
where t.rec.c.c1.ca = 3
