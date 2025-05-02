#
# Cast error (string to number)
#
insert into foo values (
    "gtm", 
    1,
    150,
    {
      "a" : 10,
      "b" : 30 
    },
    {
      "long" : 120,
      "rec2" : { "str" : 15, "num" : "xyz", "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
