select id, t.info.age
from foo t
where t.info.address.state = null
