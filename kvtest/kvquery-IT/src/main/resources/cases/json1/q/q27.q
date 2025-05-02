
select $obj, $obj is of type (only MAP(ANY))
from foo, { "id" : id, "arr" : [record] } $obj
