select /* FORCE_INDEX(Foo idx_d_f) */id
from Foo t
where t.rec.f = 4.5 and exists t.rec.d[11 < $element.d2 and $element.d2 < 20]
