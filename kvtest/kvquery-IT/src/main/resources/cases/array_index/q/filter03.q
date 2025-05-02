select id
from foo f
where exists f.rec.b[10 < $element and $element <= 20]
