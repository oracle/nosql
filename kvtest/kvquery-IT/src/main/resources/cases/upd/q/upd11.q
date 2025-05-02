update Foo f
set f.info.address.phones[] = { "areacode" : 450,  "number" : 8000 }
where id = 10

select f.info.address
from Foo f
where id = 10
