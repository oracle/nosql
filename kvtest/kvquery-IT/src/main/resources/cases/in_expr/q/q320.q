declare $arr15 array(json); // [ (8, 3.4), (2, 3.9), (9, 3.0) ]
        $arr16 array(json); // [ 3.4, 3.5 ]
select id
from foo f
where (f.info.bar1, f.info.bar2) in $arr15[] and
      f.info.bar2 in $arr16[]

