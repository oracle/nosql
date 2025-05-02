select /*+ FORCE_INDEX(Foo idx_c1_c2_c3) */id, g
from Foo f
where f.rec.c.c1.ca >= 1 and f.rec.c.c2.ca = 3
