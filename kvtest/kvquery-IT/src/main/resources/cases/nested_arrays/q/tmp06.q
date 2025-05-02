select id
from foo f
where exists f.info.array1[][ 3 <= $element.a and $element.a <= 4]
