declare $arr11 array(json); // [ (4, "d"), (3, "g") ]
        $k5 integer; // 100
        $k6 integer; // 108
select id
from foo f
where f.info.bar1 in (7, 2, null) and
      (foo1, f.info.bar3) in $arr11[] and
      $k5 <= f.info.bar4 and f.info.bar4 < $k6
