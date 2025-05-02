select id, record
from foo f
where f.record.float > 0
order by f.record.float
