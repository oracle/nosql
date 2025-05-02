
select $obj, $obj is of type (only MAP(JSON))
from Foo f, 
         [[1,2,3], [1, 2]][0] $arr,
         { "id" : id, "arr1" :  $arr, "arr2" : $arr } $obj
