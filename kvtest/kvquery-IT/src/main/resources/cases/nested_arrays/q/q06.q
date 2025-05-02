select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where exists f.info.addresses.phones[$element.areacode = 408 and $element.kind = "home"]
