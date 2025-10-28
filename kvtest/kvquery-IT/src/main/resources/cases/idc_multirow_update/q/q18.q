update Foo f
set f.record.int = $ /10 + 100,
set f.info.address.phones[].areacode = $ + 1,
set ttl 10 days,
add f.info.address.phones seq_concat({ "areacode" : 570,  "number" : 51 }, { "areacode" : 580,  "number" : 51 }),
put f.info.children seq_concat({"Rahul": 22},{"Trump": null}),
remove f.info.lastName
where sid = 0

select $f, remaining_days($f) as remaining_days
from Foo $f
where sid = 0