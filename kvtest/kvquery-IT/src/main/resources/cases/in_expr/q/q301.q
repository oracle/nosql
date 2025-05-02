declare $arr3 array(json); // [6, 3, null]
select id
from foo f
where f.info.bar1 in $arr3[]
