# Test Description: record operand1 > array operand2.

select id1, Foo.rec > Foo.arr
from Foo
order by id1
