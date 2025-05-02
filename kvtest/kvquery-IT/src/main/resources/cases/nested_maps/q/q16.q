select id
from foo f
where exists f.info.addresses.phones[not exists $element.values($value.values().areacode =any 500)]
