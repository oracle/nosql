declare $arr9 array(json); // [ 7, 2, null ]
        $arr11 array(json); // [ 4, "d", 3, "g" ]
select id
from foo f
where f.info.bar1 in $arr9[] and
      (foo1, ltrim(f.info.bar3)) in $arr11[] and
      100 <= f.info.bar4 and f.info.bar4 < 108 
