
select $obj, $obj is of type (only MAP(JSON))
from Foo f, { "id" : id, "arr" : [[1,2,3], [1, 2]][0] } $obj
