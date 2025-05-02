select /*+ FORCE_INDEX(Foo idx_c1_keys_vals_c3) */id
from Foo t
where exists t.rec.c.keys($key in ("c1", "c4") and $value.ca in (1, 3))
