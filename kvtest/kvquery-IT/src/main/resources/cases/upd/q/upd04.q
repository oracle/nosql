update Foo f
set f.record.map1 =
    { "foo" : { "f1" : { "f1f1" : 1 }, 
                "f2" : { "f2f1" : 4, "f2f2" : 5 } 
              } 
    },
json merge f.info.children.George.friends with patch [ "Mark", "John" ],
set f.record.map2 =
    { "bar" : [ 1, 2, 3] },
set f.info.address.phones = [],
add f.info.address.phones { "areacode" : 400, "number" : 3445, "kind" : "work" },
add f.info.address.phones 0 { "areacode" : 600, "number" : 13132, "kind" : "home" }
where id = 3


select f.record, 
       f.record.map1.foo.f1 is of type (map(integer)) as typecheck1,
       f.record.map1.foo.f2 is of type (only map(json)) as typecheck2,
       f.info.address.phones,
       f.info.children.George
from Foo f
where id = 3
