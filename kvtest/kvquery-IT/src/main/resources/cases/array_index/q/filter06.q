select id
from foo f
where exists f.rec[$element.a = 10].b[10 < $element and $element <= 20]
