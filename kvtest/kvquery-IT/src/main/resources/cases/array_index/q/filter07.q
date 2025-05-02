select /* FORCE_INDEX(Foo idx_a_c_f) */ id
from foo f
where exists 
      f.rec[$element.a = 10 and $element.c.ca =any 3].b[10 < $element and $element <= 20]
