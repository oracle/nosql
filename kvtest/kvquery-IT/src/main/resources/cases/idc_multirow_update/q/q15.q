update jsoncol j
add j.address[$element.phones is of type (ARRAY(any))].phones
    { "areacode" : $areacode, "number" : $number, "kind" : $kind },
set j.address[$element.phones is of type (MAP(any))].phones =
    [$.values(), { "areacode" : $areacode, "number" : $number, "kind" : $kind }]
where majorKey1 = "j1" and majorKey2 = "j2"

select minorKey, address
from jsoncol
where majorKey1 = "j1" and majorKey2 = "j2"