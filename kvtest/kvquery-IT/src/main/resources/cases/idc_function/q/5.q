# Test Description: Pass ANY as argument to size() function.
# Type: Negative

select id1, size(f.ANY)
from foo f
order by id1