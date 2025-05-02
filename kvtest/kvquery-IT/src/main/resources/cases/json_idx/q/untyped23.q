select id
from bar b
where b.info.address.state > 5 and b.info.address.state = "CA"
