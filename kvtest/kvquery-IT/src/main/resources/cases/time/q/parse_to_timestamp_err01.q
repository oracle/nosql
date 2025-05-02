select parse_to_timestamp(t.info.dt.str, 'MM/dd yyyy') 
from arithtest t 
where id = 0
