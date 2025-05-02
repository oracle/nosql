select *
from foo t
order by t.info.address.state, t.info.address.city, t.info.age, id
