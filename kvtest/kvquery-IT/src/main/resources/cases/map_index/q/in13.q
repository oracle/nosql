select /*+ FORCE_INDEX(Foo idx_c1_keys_vals_c3) */id
from Foo t
where exists t.rec.c.keys(($key, $value.ca) in (("c1", 1), ("c2", 3)))

