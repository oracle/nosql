# Test Description: Pass Array as argument to size() function with exists operator.
# Type: Negative
select id1, size(exists arr)
from Foo
order by id1