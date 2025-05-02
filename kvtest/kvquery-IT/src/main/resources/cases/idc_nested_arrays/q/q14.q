select id 
from nestedTable nt
where nt.addresses[exists $element.phones[$element.number >any 60]].phones.kind =any "work" 
