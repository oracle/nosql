select /*+ FORCE_INDEX(Foo idx_b) */ id
from foo f
where exists f.rec[$element.f = 4.5].b[10 < $element and $element <= 20]
