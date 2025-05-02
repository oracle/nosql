declare $arr9 array(json);  // [ 7, 2, null ]
select id
from foo f
where f.info.bar1 in $arr9[] and
      (foo1, f.info.bar2) in ((7, 3.5), (4, 3.9)) and
      "a" <= f.info.bar3 and f.info.bar3 < "p"
