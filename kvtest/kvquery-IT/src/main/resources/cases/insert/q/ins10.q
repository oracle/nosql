insert into foo f (str, rec1, id2, info, id1) values (
    null, 
    {
      "long" : 5,
      "rec2" : { "str" : null, "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : null,
      "map" : { "foo" : "xyz", "bar" : "dff" }
    },
    110,
    default,
    1
)
