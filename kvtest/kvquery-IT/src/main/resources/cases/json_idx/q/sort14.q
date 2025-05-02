select id, b.record.long + b.record.int as sum
from bar b
order by b.record.long + b.record.int, id


