
insert into foo values ( 
    "gtm", 
    1, 
    101, 
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

select * 
from foo
where id2 = 101
