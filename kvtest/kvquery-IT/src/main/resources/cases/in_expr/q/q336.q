declare $arr25 array(json); // [ 3, 5 ]
select id
from foo f
where exists f.info.phones[$element.num in $arr25[] and $element.kind > "f"]
