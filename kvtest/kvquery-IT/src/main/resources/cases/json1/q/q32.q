select id, f.info.address.phones.keys().keys($key = "areacode" and $value = "408") 
from foo f 
order by id
