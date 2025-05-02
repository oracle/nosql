select /*+ FORCE_INDEX(Foo idx_ca_f_cb_cc_cd) */ id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 1 and t.rec.c.c2.ca = 3 and
      t.rec.f = 4.5 and
      t.rec.c.c2.cb > 20 and t.rec.c.c1.cb <= 33 and t.rec.c.c2.cb < 42 and
      t.rec.c.c1.cc >= 101
