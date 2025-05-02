#
# range only, order-by done by sort iter.
#
select *
from foo t
where "MA" <= t.address.state
order by id desc
