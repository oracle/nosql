select id 
from nestedTable $nt
where $nt.addresses.values().phones.values().keys() =any "phone1" and $nt.age = 17
