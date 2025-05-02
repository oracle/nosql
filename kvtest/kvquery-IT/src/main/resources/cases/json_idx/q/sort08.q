select id, f.info.address.state
from Foo f
order by f.info.address.state, id
