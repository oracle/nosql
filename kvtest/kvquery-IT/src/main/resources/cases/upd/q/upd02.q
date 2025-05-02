update Foo f
json merge f.info.address with patch { "street" : "somewhere" },
           f.info.children.Mark with patch { "hobbies" : [ { "football" : "defence"},
                                                           "hokey"
                                                         ],
                                             "otherParent" : { "firstName" : "john" }
                                           },
set f.info.address.phones[$element.areacode > 400].number = $ + 2,
set f.info.children.values().age = $ * 2,
set f.info.children.Anna = { "age" : $.age + 1,  "school" : "sch_2", "friends" : []}
where id = 1


select *
from Foo
where id = 1
