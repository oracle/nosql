# Array filter stepenum
# Test Description: Context item is binary.

select id, C.bin[$pos=0]
from Complex C
order by id