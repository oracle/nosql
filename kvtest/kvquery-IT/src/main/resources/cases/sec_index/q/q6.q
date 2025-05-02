#
# equality via two range preds
#
select id, age
from foo t
where "MA" <= t.address.state and
      t.address.city = "Boston" and
      "MA" >= t.address.state and
      age >= 11
