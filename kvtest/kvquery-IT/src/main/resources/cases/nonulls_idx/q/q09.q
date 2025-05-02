select id, t.info.age
from foo t
where "MA" <= t.info.address.state and id > 1
