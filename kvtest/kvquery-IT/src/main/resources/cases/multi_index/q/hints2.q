#
# a_rf and a_c_f have same score (both have full keys).
# a_rf should win because it has fewer fields but
# because of the index hint, idx_a_c_f wins
#
select /*+ PREFER_INDEXES(Foo idx_a_c_f) */ *
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and t.rec.f = 4.5
