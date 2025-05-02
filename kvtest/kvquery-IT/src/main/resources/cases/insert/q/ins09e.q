insert into foo (str, rec1, id2, id1) values (
    $str2, 
    {
      "long" : $long,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : { },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    },
    309,
    $id1
) set ttl 0 hours

select $f as row, remaining_days($f) as dttl, remaining_hours($f) as httl
from foo $f
where id2 = 309
