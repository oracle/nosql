insert into foo values (
    "gtm", 
    1, 
    302, 
    $info,
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "num" : 23E2, "arr" : [ 10, 4, 6 ] },
      "recinfo" : parse_json($recinfo2),
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)

select *
from foo
where id2 = 302
