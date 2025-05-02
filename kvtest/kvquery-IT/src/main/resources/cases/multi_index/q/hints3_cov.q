#
# covering index a_c_f overrides preferred index.
#
select  /*+ PREFER_INDEXES(Foo idx_a_rf) */  id1
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and id2 > 0
