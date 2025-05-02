select /*+ FORCE_INDEX(Foo idx_c1_keys_vals_c3) */id
from Foo f
where f.rec.c.c1.ca in (3, 10) and exists f.rec.c.keys($key in ("c4", "c3"))
