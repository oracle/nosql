declare $arr3 array(json); // [ 6, 3, null ]
select id
from foo f
where f.info.bar1 in $arr3[] and
      f.info.bar2 in (3.5, 3.6, null, 3.4, 3.0)
