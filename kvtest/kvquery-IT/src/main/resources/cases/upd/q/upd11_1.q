
update Foo f
set f.info.address.phones[0] = { "areacode" : 450,  "number" : 8000 }
where id = 10
returning f.info.address.phones
