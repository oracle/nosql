select /* FORCE_INDEX(Foo idx_d_f) */ id
from Foo t
where t.rec[$element.f = 4.5].d[11 < $element.d2 and $element.d2 < 20].d3 =any 13
