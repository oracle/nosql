# Test Description: Passing int column in L & H expression

select id, $C.address.phones[id : age]
from Complex $C
order by id