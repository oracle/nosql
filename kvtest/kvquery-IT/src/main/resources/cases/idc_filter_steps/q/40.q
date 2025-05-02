# Map filter step
# Test Description: Context item is float.

select id, C.flt.values()
from Complex C
order by id