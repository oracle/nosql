# Array filter stepenum
# Test Description: Context item is fixed binary.

select id, C.fbin[$pos=0]
from Complex C
order by id