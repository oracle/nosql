select id
from foo f
where exists f.info.addresses.phones[][][$element.areacode = 408 and $element.number > 30]
