select id
from foo f
where exists f.info.addresses.phones.values($value.values().areacode = 510)
