# Test Description: operand1 return NULL using >= operator.

select foo.rec >= foo.arrrec[0]
from foo
where id1 = 1