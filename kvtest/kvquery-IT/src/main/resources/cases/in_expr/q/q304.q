declare $arr5 array(json); // [ 4, 3, null ]
        $arr6 array(json); // [ 3.5, 3.6, null, 3.1, 3.2 ]
        $k4 string; // "p"
select id
from foo f
where f.info.bar1 in $arr5[] and
      f.info.bar2 in $arr6[] and
      "c" <= f.info.bar3 and f.info.bar3 < $k4
