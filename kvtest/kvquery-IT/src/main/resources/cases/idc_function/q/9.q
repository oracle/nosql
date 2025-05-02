# Test Description: Pass STRING_CONST argument to size() function.

select id1, size("test")
from Foo
order by id1