select id
from Foo t
where exists t.rec[$element.f = 4.5].d[$element.d2 = 20]
