insert into foo values (
    default, 
    1, 
    104,
    default,
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "arr" : [ 10, 4, 6 ] },
      "recinfo" : { "a" : 4, "b" : "str" },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)

select $f as row, remaining_days($f) as ttl
from foo $f
where id2 = 104
