#
# a_rf and a_c_f have same score.
# a_rf wins (fewer fields)
#
select id1, id2, id3
from Foo t
where 0 < t.rec.a and t.rec.f >= 0
