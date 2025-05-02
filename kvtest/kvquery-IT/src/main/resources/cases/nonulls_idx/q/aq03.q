# complete key
select id
from Foo f
where f.info.address.state = "MA" and
      f.info.address.phones.areacode =any 520 and
      f.info.age = 11
