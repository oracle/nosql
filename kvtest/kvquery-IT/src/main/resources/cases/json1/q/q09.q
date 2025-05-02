#
# Map constructor
# Input : integer, maps, single map, jnull
#
select { "id" : id, "phones" : f.info.address.phones[0:2]}
from Foo f


