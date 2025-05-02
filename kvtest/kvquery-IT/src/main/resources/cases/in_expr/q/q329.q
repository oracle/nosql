declare $arr9 array(json); // [ 7, 2, null ]
        $arr24 array(json); // [ (4, 3.9, "d"), (3, 4.0, "g") ]
select id
from foo f
where f.info.bar1 in $arr9[] and
      (foo1, f.info.bar2, ltrim(f.info.bar3)) in $arr24[] and
      100 <= f.info.bar4 and f.info.bar4 < 108
