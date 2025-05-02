#
# Array Filter:
# Input: array, jnull, map
# $element : map, numeric, jnull
# comparison with jnull
#
select f.info.address.phones[500 < $element.areacode and $element.areacode < 600] 
from foo f

