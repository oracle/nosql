update Foo f
set f.info.address.phones[0].number = $ + f.info.address.phones[1].number,
    f.record.long = 100,
    f.record.values($value = 50) = 100,
    f.info.children = { "Anna" : {"age" : 10,  "school" : "sch_2", "friends" : []}},
json merge f.info.address with patch { "city" : null }
where id = 2


select *
from Foo
where id = 2
