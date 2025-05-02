select id 
from nestedTable nt
where nt.age = 30 and nt.addresses.phones[][].areacode =any 304
