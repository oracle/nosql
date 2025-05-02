select id
from foo f
where exists f.info.array1[ 3 <=any $element.a and $element.a <=any 4 ]
