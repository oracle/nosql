
select id, $arr, $arr is of type (ARRAY(JSON))
from foo f, [ cast (f.map.values() as ARRAY(JSON)*) ] $arr
order by id
