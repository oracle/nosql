# Test Description: operand1 return EMPTY using != operator.

select id1, foo.map.key10 != "key10"
from foo
order by id1