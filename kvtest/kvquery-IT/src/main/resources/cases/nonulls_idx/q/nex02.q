select id
from foo f
where not exists f.info.address.state and not exists f.info.age
