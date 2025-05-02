#
# nothing pushed
#
select *
from foo t
order by t.address.state, t.address.city, t.age
