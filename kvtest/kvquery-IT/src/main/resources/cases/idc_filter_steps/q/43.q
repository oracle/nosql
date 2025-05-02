# Map filter step
# Test Description: Context item is boolean.

select id, C.bool.values()
from Complex C
order by id