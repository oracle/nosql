#
# Array Slice step:
# Input : array, jnull, map
#
# Array constructor
# Input: map, empty, atomics
#
select [ f.info.address.phones[2:10] ]
from foo f
order by id
