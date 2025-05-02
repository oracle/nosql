#
# always false
#
select id, t.info.age
from Foo t
where "S" < t.info.address.city and
      t.info.address.city = "Boston" and
      "MA" = t.info.address.state and
      t.info.age > 50
