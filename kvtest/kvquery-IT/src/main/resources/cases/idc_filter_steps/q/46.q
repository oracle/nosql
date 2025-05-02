# Map filter step
# Test Description: Context item is Fixed Binary.

select id, C.fbin.values()
from Complex C
order by id