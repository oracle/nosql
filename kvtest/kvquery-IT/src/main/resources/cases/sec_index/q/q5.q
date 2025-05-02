#
# key gap
#
select id, age
from foo t
where t.address.state = "CA" and
      t.age = 10
