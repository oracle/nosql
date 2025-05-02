declare $arr23 array(json); // [ (7, 3.0), (7, 3.5), (3, 3.6) ]
        $k12 string; // 't'
        $k7 integer; // 7
select id
from foo f
where f.info.bar1 = $k7 and
      (f.info.bar1, f.info.bar2) in $arr23[] and
      3 <= f.info.bar2 and f.info.bar2 <= 3.5 and
      f.info.bar3 <= $k12
