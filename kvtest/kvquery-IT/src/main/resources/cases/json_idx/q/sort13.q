select id
from bar b
where b.info.address.state = "OR"
order by id
limit 3
offset 2
