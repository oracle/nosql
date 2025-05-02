#
# range only
#
select id, t.address.state, t.address.city 
from foo t
where "MA" <= t.address.state
order by t.address.state, t.address.city
