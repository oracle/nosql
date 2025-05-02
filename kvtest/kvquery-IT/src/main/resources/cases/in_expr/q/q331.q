declare $arr26 array(json); // [ (3, "a"), (5, "b") ] 
select id
from foo f
where exists f.info.phones[($element.num, $element.kind) in $arr26[]]
