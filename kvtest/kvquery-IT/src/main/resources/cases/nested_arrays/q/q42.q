select id
from foo f
where f.info.addresses[$element.phones[][][$element.areacode = 408 and $element.kind = "work"]].state =any "MA"
