select id
from nestedTable nt
where nt.addresses[].city =any "Boston" and nt.addresses.phones.number <any 31
