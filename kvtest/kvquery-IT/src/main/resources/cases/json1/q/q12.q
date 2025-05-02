#
# Map constructor with variable field names, empty field names, NULL field values
# homogeneous array constructor
#
select { "id" : id, 
         f.info.children.John.school : f.record.long,
         f.record.string : [1, 2, 3] }
from Foo f

