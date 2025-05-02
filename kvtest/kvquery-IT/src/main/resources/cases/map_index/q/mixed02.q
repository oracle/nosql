select /*+ FORCE_INDEX(Foo idx_c1_keys_vals_c3) */id
from Foo f
where f.rec.c.c1.ca = 3 and f.rec.c.keys() =any "c4"
