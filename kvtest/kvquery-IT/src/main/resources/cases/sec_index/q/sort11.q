select id, age
from foo t
where t.address.state > "X"
order by t.address.state, t.address.city, t.age
