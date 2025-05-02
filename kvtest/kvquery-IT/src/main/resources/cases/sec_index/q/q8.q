#
# range only
#
select id, age
from foo t
where "MA" <= t.address.state
