# Map filter step
# Test Description: Context item is long.

select id, C.lng.values()
from Complex C
order by id