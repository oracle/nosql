#
# complete key
#
select id, t.info.age
from foo t
where t.info.address.state = "CA" and
      t.info.address.city = "San Fransisco" and
      t.info.age = 10
