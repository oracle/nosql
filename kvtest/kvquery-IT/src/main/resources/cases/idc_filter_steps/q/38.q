# Map filter step
# Test Description: Context item is integer.

select id, C.age.values()
from Complex C
order by id