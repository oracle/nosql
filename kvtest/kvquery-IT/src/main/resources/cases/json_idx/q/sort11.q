select id
from bar b
where b.info.address.state = "CA"
order by  b.info.address.state, id
