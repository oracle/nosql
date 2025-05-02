select id 
from nestedTable nt
where nt.addresses[exists $element.phones[$element.number =any 50]].phones.number >any 60
