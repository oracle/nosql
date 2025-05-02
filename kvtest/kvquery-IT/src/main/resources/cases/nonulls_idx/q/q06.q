#
# equality via two range preds
#
select id, t.info.age
from foo t
where "MA" <= t.info.address.state and
      t.info.address.city = "Boston" and
      "MA" >= t.info.address.state and
      t.info.age >= 11
