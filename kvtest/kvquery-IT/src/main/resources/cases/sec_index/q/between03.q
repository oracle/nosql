select id
from foo t
where t.address.state = "MA" and t.address.city between "T" and "C"
