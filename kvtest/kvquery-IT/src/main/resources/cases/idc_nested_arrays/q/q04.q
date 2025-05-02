select id
from nestedTable nt
where nt.addresses.phones.number >any 70 
