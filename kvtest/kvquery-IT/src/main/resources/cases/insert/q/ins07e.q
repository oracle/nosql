insert into foo (str, rec1, id2, id1) values (
    $str2, 
    {
      "long" : $long,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    },
    307,
    $id1
)
returning *
