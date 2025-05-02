select id
from foo f
where exists f.info.addresses.phones[exists $element.values($value.areacode =any 104)]
