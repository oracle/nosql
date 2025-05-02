#
# a_c_f should win with highest score (more equality preds) but
# force index hints selects a_rf
#
select  /*+ PREFER_INDEXES(Foo idx_a_rf) */  *
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and id2 > 0
