#
# Map Filter
# Input: map, empty map, sequence(map, atomics), jnull
# $value : integer, string
# compute-per-map condition
# compute-once condition
#
select { "id" : id,
         "phone_keys" : f.info.address.phones[].values($key != "number" and $value < 408),
         "children" : f.info.children.values(size($) > 2).age,
         "alway_false" : f.info.children.keys(false),
         "always_true" : f.info.children.keys(true)
       }
from foo f
order by id
