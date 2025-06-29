#
# Row does not exist. No SET TTL
#
upsert into foo $f values (
    "gtm", 
    1,
    -201,
    {
      "a" : 10,
      "b" : 30 
    },
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
returning $f as row, remaining_days($f) as ttl

