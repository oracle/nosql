# Test Description: Select existing field from a record.

select id, C.address.city, C.address.state, C.address.ptr
from Complex C
order by id