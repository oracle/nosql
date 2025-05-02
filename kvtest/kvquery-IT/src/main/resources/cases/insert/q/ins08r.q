insert into foo f (str, rec1, id2, info, id1) values (
    null, 
    {
      "long" : 5,
      "rec2" : { "str" : null, "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : null,
      "map" : { "foo" : "xyz", "bar" : "dff" }
    },
    208,
    null,
    1
)
returning str is null as str, 
          info = null as info, 
          f.rec1.rec2.str is null as str2,
          f.rec1.recinfo = null

