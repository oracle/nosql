#
# a_rf and a_c_f have same score.
# a_rf wins (fewer fields)
#
select *
from Foo t
where 0 < t.rec.a and t.rec.f >= 0
