#
# range only, but primary index is used due to order-by.
#
select *
from foo t
where "MA" <= t.info.address.state
order by id desc
