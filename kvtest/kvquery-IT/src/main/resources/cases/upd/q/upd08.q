update Foo f
set f.info.address.phones[$element.areacode = 408 and
                          $element.number >= 5000000].areacode = 409,
remove f.info.address.phones[$element.areacode = 408],
add f.info.address.phones 3 { "areacode" : 650,  "number" : 400 }
where id = 7


select f.info.address
from Foo f
where id = 7
