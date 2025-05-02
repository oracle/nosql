insert into foo f values (
    "gtm", 
    1, 
    302, 
    $info,
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : parse_json("{\"a\":10}"),
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
returning f.info, f.rec1
