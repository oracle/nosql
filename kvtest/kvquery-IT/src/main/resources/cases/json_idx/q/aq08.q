# should throw exception
select id
from Foo f
where f.info.address.state = "CA" and f.info.address.phones.areacode > 510
