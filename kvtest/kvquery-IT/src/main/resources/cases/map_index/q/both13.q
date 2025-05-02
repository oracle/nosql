select /*+ FORCE_INDEX(Foo idx_ca_f_cb_cc_cd) */ id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca >= 1 and (t.rec.c.c1.cd < -10 or t.rec.c.c1.cd >= -1)
