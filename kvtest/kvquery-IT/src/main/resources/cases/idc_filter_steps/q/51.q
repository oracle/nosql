# Array filter step
# Test Description: Context item is string.

select id, C.firstName[$element = "firstName"]
from Complex C
order by id