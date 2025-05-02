update Foo f
add f.info.address.phones ([ { "areacode" : 650,  "number" : 400 },
                             { "areacode" : 650,  "number" : 400 } ][])
where id = 30
returning *
