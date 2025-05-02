select id, {"id":f.id, "record" : f.record, "info":f.info}.info.address.phones.areacode
from foo f 
order by id
