declare $arr22 array(json); // [ (7, 3.0), (4, 3.4), (3, 3.6) ]
        $k11 double; // 3.8
select id
from foo f
where f.info.bar1 = 7 and
      (f.info.bar1, f.info.bar2) in $arr22[] and
      3.1 <= f.info.bar2 and f.info.bar2 < $k11
