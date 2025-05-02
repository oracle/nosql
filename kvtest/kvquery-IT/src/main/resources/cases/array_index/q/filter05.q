select id
from foo f
where exists f.rec[$element.c.ca =any 3].b[10 < $element and $element <= 20]
