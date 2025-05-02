declare $arr4 array(json); // [ 3.5, 3.6, null, 3.4, 3.0 ]
select id
from foo f
where f.info.bar1 in ( 6, 3, null ) and
      f.info.bar2 in $arr4[] and
      exists f.info.bar3
