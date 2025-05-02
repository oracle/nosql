#
# EXISTS
# json null literal in map constructor
#
select { "id" : id,
         "null" : null,
         "exists1" : exists f.info.address.phones,
         "exists2" : exists f.record.long,
         "exists3" : exists f.info.children.John,
         "exists4" : not exists f.info.address.phones[$element.areacode = null],
         "empty" : {}
       }
from foo f
