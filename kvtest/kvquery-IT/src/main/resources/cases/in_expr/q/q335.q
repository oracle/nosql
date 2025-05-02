declare $arr25 array(json); // [ 3, 5 ]
        $arr28 array(json); // [ (4, "h"), (6, "c"), (6, "s")]
select id
from foo f
where exists f.info.phones[$element.num in $arr25[]] and
      exists f.info.phones[($element.num, $element.kind) in $arr28[]]
