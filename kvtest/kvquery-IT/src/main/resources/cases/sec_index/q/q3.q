#
# always false
#
select id, age
from Foo t
where "S" < t.address.city and
      t.address.city = "Boston" and
      "MA" = t.address.state and
      age > 50
