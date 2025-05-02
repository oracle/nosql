select id, f.record.keys(f.record.bool) 
from foo f 
where f.record.keys(f.record.bool) = null 
order by id
