# Map filter step
# Test Description: Context item is Enum.

select id, C.enm.values()
from Complex C
order by id