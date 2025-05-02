#
# Even thoug a_id2 is preferred, a_c_f wins because its covering
# and has a pushed equality pred (actually 2 of them).
#
select /*+ PREFER_INDEXES(Foo idx_a_id2) */ id1
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3
