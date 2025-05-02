# Test Description: array operand1 < record operand2.

select id1, Foo.arr < Foo.rec
from Foo
order by id1
