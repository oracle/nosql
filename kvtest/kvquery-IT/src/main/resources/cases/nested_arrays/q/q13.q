select id
from foo f
where exists f.info.addresses.phones[$element.areacode =any 408][][$element.number > 30]
