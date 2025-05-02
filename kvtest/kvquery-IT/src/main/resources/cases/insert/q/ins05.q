insert into foo values (
    default, 
    1, 
    105,
    default,
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "arr" : [ 10, 4, 6 ] },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
set ttl 3 hours

select $f as row, remaining_days($f) as dttl, remaining_hours($f) as httl
from foo $f
where id2 = 105
