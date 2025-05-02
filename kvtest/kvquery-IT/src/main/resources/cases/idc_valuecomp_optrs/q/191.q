# Test Description: record operand1 < map operand2.

select id1, Foo.rec < Foo.map
from Foo
order by id1
