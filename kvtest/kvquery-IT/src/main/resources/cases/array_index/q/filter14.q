select /* FORCE_INDEX(Foo idx_d_f) */ id
from Foo t
where t.rec.d[11 < $element.d2].d2 <any 20

