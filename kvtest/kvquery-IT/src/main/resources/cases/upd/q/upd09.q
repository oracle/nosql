update Foo f
add f.info.address[$element.phones is of type (array(any))].phones 
    { "areacode" : 650, "number" : 80 },
set f.info.address[$element.phones is of type (map(any))].phones =
    [ f.info.address.phones, { "areacode" : 650, "number" : 80 } ]
where id = 8


select f.info.address.phones
from foo f
where id = 8
