select id, age
from foo t
where "MA" <= t.address.state and id > 1
