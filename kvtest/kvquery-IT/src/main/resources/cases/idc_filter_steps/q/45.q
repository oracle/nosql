# Map filter step
# Test Description: Context item is Binary.

select id, C.bin.values()
from Complex C
order by id