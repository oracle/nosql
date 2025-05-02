select id
from foo f
where exists f.info.addresses.phones.values($value.phone1.areacode = 510)
