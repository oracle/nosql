# Test Description: Name expression returns empty sequence

select id, C.address.city.foo
from Complex C
order by id
