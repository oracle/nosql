declare $str1 string; // gmt
        $long long;  // 120
insert into foo values (
    $str1, 
    1, 
    301, 
    {
      "a" : 10,
      "b" : 30 
    },
    {
      "long" : $long,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)

select * 
from foo
where id2 = 301
