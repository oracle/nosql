select parse_to_timestamp(t.info.dt.str, t.info.dt.pattern, 'EST') 
from arithtest t 
where id = 0
