#
# Array Filter:
# Input: array, jnull, map
# $element : map, numeric, jnull
#
# Array constructor
# Input: map, empty
#
select [ f.info.address.phones[50 < $element.number and $element.number < 60] ]
from foo f
