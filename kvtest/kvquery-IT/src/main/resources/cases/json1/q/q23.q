#
# KEYS() step
# Input: map, empty map, sequence(map, atomics)
#
select { "id" : id,
         "phone_keys" : f.info.address.phones[].keys(),
         "children" : f.info.children.keys()
       }
from foo f
where NOT (id = 1 OR id = 4)

