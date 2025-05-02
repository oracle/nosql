declare $arr7 array(json); // [ (8, "a"), (4, "a"), (6, ""), (9, null), (null, null) ]
        $arr8 array(json); // 
select id
from foo f
where (f.info.bar1, f.info.bar3) in $arr7[] and
      f.info.bar2 in (3.9, null, 3.1, 3.2) and
      101 <= f.info.bar4 and f.info.bar4 < 108 
