select id
from foo f
where exists f.info[$element.age = 10].addresses.phones[][][$element.areacode > 500] and
      f.info.addresses.phones.kind =any "home"
