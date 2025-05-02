declare $arr27 array(json); // [ "a", "b" ]
        $arr25 array(json); // [ 3, 5 ]
select id
from foo f
where exists f.info.phones[$element.num in $arr25[]] and
      exists f.info.phones[$element.kind in $arr27[]]
