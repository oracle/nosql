#
# always false
#
select id, t.info.age
from foo t
where "M" > t.info.address.state and
      "S" < t.info.address.city and
      t.info.address.city = "Boston" and
      t.info.age > 50
