#
# a_rf and a_c_f have same score (both have full keys).
# a_c_f wins because it's covering
#
select id1
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and t.rec.f = 4.5
