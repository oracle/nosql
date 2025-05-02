#
# Map constructor with variable field names and empty field values
#
select { "id" : id, 
         "phones" : f.info.address.phones[3:4],
         f.record.string : case when f.record.int > 0 then f.record.int else 0 end }
from Foo f

