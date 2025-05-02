select /*+ PREFER_INDEXES(Foo idx_a_c_f idx_a_rf) */ *
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and t.rec.f = 4.5
