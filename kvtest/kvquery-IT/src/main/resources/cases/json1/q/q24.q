#
# Map Filter
# Input: map, empty map, array(map, atomics), jnull
# $value : integer, string
#
select { "id" : id,
         "phone_keys" : f.info.address.phones.values($key != "number" and $value < 408),
         "children" : f.info.children.keys($key != "John")
       }
from foo f
