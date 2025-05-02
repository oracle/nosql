declare $str1 string; // gmt
        $long long;  // 120
insert into foo (str, id1, info, rec1, id2) values (
    $str1, 
    1,  
    {
      "a" : 10,
      "b" : 30 
    },
    {
      "long" : $long,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    },
    306
)
returning *

