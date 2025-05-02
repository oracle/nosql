# Array filter stepenum
# Test Description: Context item is Enum.

select id, C.enm[$element = "tok1"]
from Complex C
order by id