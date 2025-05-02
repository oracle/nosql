#
# range only
#
select id, t.info.age
from foo t
where "MA" <= t.info.address.state
order by t.info.address.state, t.info.address.city, t.info.age
