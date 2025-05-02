select /* FORCE_PRIMARY_INDEX(foo) */ id
from foo f
where f.info.age = 10 and 
      f.info.addresses.phones[][][$element.areacode in (408, 510, 650)].kind =any "work"
