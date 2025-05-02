#
# Array Filter:
# Input: array, jnull, map
# $element : map, numeric, jnull
#
# Array constructor
# Input: map, empty, jnull
#
select [ f.info.address.phones[$element = null or
                               (50 < $element.number and $element.number < 60) ] 
       ]
from foo f


