#
# complete key
#
select id, age
from foo t
where t.address.state = "CA" and
      t.address.city = "San Fransisco" and
      t.age = 10
