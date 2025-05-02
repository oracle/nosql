update Foo f
set f.info.age = $ + 2,
set f.info.lastName = "XYZ",
json merge f.info with patch { "address" : { "state" : "OR" }, "firstName" : "abc" }
where id = 0


select *
from Foo
where id = 0
