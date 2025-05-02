#
# KEYS() step
# Input: map, empty map
#
select id, f.info.address.keys(), f.info.children.keys()
from foo f
