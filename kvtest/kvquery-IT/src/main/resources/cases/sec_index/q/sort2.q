#
# range only
#
select id, age
from foo t
where "MA" <= t.address.state
order by t.address.state, t.address.city, t.age
