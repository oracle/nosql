#
# key gap, nothing pushed
#
select id
from Foo t
where t.rec.d[0:1].d2 =any t.rec.b[2] and t.rec.f = 4.5
