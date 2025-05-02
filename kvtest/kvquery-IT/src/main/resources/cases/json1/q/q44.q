select id, {"id":f.id, "record":f.record, "info":f.info}.record.values() 
from foo f 
order by id
