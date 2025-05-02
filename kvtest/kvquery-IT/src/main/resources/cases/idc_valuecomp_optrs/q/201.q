# Test Description: map operand1 >= record operand2.

select id1, Foo.map >= Foo.rec
from Foo
order by id1
