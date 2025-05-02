# Test Description: Pass ANYATOMIC as argument to size() function.
# Type: Negative

select id1, size(f.ANYATOMIC)
from foo f
order by id1