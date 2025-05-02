select id
from foo f
where f.info.addresses[exists $element.phones[$element.kind =any "work"]].phones.areacode =any 408
