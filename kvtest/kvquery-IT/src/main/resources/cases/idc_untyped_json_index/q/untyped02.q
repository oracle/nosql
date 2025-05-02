select id, b.info.address.state as state
from bar b
where b.info.address.state < "WA"
order by b.info.address.state DESC NULLS FIRST
