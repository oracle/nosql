select id, 
       f.record.bool, 
       { "id":f.id, 
          "record":f.record, 
          "info":f.info
       }.info.address.phones.values(f.info.NO_SUCH_KEY) 
from foo f
order by id
