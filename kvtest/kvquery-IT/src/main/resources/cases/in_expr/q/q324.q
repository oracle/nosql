declare $arr20 array(integer); // [ 7, 4, 3]
        $arr21 array(json);    // [ 3.0, 3.4, 3.6 ]
        $k7 integer; // 7
select id
from foo f
where f.info.bar1 = $k7 and
      f.info.bar1 in $arr20[] and
      f.info.bar2 in $arr21[] and
      f.info.bar2 >= 3.4
