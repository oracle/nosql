select /*+ FORCE_INDEX(Foo idx_ca_f_cb_cc_cd) */ id
from Foo t
where t.g = 5 and t.rec.c."values()".ca =any 10
