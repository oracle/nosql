#
# KEYS() step
# Input: map, empty map, array(map, atomics)
#
select { "id" : id,
         "phone_keys" : f.info.address.phones.keys(),
         "children" : f.info.children.keys()
       }
from foo f
where NOT (id = 1 OR id = 4)
order by id

