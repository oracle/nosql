declare $arr17 array(double); // [ 3.0, 3.1, 3.5, 3.9 ]
        $arr18 array(json);   // [ (3.1, "g"), (3.9, "d") ]
select id
from foo f
where f.info.bar1 = 7 and
      f.info.bar2 in $arr17[] and
      (f.info.bar2, f.info.bar3) in $arr18[]
