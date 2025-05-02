declare $arr29 array(json); // [ "a", "b", "c" ]
select id
from foo f
where exists f.info.phones[$element.num = 5 and $element.kind in $arr29[]]
