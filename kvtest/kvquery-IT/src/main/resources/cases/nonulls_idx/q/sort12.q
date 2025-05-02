select id, record
from foo f
where f.record.float > 0 or f.record.float is null
order by f.record.float
