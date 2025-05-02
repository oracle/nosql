select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where f.info[$element.addresses.phones[][][$element.number >= 54].areacode >any 408].age = 10
