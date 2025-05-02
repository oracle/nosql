#
# key gap
#
select id, t.info.age
from foo t
where t.info.address.state = "CA" and
      t.info.age = 10
