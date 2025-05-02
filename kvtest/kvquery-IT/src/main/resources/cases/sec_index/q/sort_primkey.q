#
# range only
#
select id, age
from foo t
where id = 3
order by t.address.state, t.address.city, t.age
