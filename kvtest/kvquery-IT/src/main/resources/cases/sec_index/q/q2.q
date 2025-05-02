#
# always false
#
select id, age
from foo t
where "M" > t.address.state and
      "S" < t.address.city and
      t.address.city = "Boston" and
      age > 50
