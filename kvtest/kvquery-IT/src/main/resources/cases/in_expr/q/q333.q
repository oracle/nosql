declare $arr27 array(json); // [ "a", "b" ]
select id
from foo f
where exists f.info.phones[$element.kind in $arr27[]]
