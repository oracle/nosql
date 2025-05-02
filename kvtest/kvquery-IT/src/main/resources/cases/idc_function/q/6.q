# Test Description: Pass ANYRECORD as argument to size() function.
# Type: Negative

select id1, size(f.ANYRECORD)
from foo f
order by id1