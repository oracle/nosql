# Test Description: map operand1 <= array operand2.

select id1, Foo.map <= Foo.arr
from Foo
order by id1
