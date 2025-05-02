#
# Default value for prim-key column
#
insert into foo values (
    "gtm", 
    1,
    default,
    {
      "a" : 10,
      "b" : 30 
    },
    {
      "long" : 120,
      "rec2" : { "str" : 15, "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
