# complete key
select id
from Foo f
where f.info.address.state = null and
      f.info.address.phones.areacode =any 408 and
      f.info.age = null
