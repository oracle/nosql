select id
from foo f
where exists f.info.addresses.phones.values().values($value.areacode = 408 and $value.kind = "home")
