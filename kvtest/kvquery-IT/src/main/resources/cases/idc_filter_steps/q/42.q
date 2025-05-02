# Map filter step
# Test Description: Context item is string.

select id, C.lastName.values()
from Complex C
order by id