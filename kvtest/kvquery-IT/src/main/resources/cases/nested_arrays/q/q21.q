select id
from foo f
where exists f.info.addresses.phones[][][$element.areacode = 415 and exists $element.number]
