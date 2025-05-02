select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where f.info.age = 10 and 
      f.info.addresses.phones[][][$element.areacode = 408].kind =any "work"
