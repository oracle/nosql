select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where f.info[exists $element.addresses.phones[][][$element.areacode = 408 and
                                                  $element.kind = "work"]].age = 10
