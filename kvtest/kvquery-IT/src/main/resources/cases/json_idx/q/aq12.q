#
# complete key and always true pred
#
select id
from Foo f
where f.info.address.state = "CA" and
      f.info.address.phones.areacode >any 600 and
      f.info.address.phones.areacode =any 650 and
      f.info.age = 10
