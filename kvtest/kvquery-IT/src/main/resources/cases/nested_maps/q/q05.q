select id
from foo f
where exists f.info.addresses.phones.values().phone1[$element.areacode = 650 and $element.number > 30]
