update Foo.Child fc
set fc.str = $ || ' of parent Foo',
set fc.info.address.phones[].areacode = $ + 1,
set ttl 5 days,
add fc.info.address.phones seq_concat({ "areacode" : 570,  "number" : 51 }, { "areacode" : 580,  "number" : 51 }),
put fc.info.children seq_concat({"Rahul": 22},{"Trump": null}),
remove fc.info.lastName
where sid = 0

select $fc, remaining_days($fc) as remaining_days
from Foo.Child $fc
where sid = 0