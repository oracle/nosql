select id
from bar b
where b.info.address.state = "OR"
order by id
offset 2
