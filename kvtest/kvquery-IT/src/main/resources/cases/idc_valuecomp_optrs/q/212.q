# Test Description: array operand1 > map operand2.

select id1, Foo.arr > Foo.map
from Foo
order by id1 
