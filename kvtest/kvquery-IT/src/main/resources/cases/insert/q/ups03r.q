#
# Row does not exist. SET TTL with an empty TTL value
#
upsert into foo $f values (
    "gtm", 
    1,
    -203,
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
set ttl seq_concat() days
returning $f as row, remaining_days($f) as ttl
