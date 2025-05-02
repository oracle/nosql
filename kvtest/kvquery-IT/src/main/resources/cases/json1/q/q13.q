#
# Empty map constructor 
# Array constructor with NULL input elem
#
select { "id" : id, 
         "empty" : [],
         "array" : [f.record[:1].long] 
       }
from Foo f
order by id
