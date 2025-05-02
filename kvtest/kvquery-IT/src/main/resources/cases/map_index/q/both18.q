select /*+ FORCE_INDEX(Foo idx2_ca_f_cb_cc_cd) */ id, t.rec.c.c1
from Foo t
where t.rec.c.c1.ca = 3 and
      t.rec.c.c2.cc = 105 and t.rec.c.c2.cd = -5 and t.rec.c.c2.cb = 11
