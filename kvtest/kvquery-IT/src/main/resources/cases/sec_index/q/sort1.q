#
# nothing pushed
#
select id, age
from foo t
where t.age > 10
order by t.address.state, t.address.city, t.age
