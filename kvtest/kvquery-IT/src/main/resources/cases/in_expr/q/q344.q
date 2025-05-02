declare $arr30 array(json); // []
select id
from foo f
where f.info.bar1 in $arr30[]
