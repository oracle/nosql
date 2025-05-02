select $obj, $obj is of type (only MAP(JSON))
from Foo f, 
         [[1,2,3], { "one" : 1, "two" : 2 }] [1] $map,
         { "id" : id, "map1" :  $map, "map2" : $map } $obj
