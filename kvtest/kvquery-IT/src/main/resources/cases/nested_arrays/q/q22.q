select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where f.info[$element.addresses.phones[][][$element.kind = "work"].areacode >any 408].age = 10

# long  50       :    1, 2,    4, 5,    15
# areacode > 408 : 0, 1,    3, 4,    6,     16
# work             0, 1,       4,           16
