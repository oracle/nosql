select id
from nestedTable nt
where exists nt.addresses.phones[][$element.areacode = 480] 
